package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

func errf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func main() {
	sep := flag.String("sep", ".", "bucket separator")
	verbose := flag.Bool("v", false, "verbose output")
	tree := flag.Bool("tree", false, "dump bucket tree")
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	path := flag.Arg(0)

	f, err := os.Open(path)
	if err != nil {
		errf("error opening db: %v", err)
		os.Exit(1)
	}
	f.Close()

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		errf("error opening db: %v", err)
		os.Exit(1)
	}
	defer db.Close()

	c := &cli{
		db:      db,
		sep:     *sep,
		verbose: *verbose,
	}

	if *tree {
		if err := c.dumpTree(); err != nil {
			errf("error: %v", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	switch flag.NArg() {
	case 1:
		// List buckets
		err = c.listBuckets()
	case 2:
		// List keys in bucket
		bucket := flag.Arg(1)
		err = c.listKeys(bucket)
	case 3:
		// View value for key in bucket
		bucket := flag.Arg(1)
		key := flag.Arg(2)
		err = c.getKey(bucket, key)
	case 4:
		// Set key in bucket
		bucket := flag.Arg(1)
		key := flag.Arg(2)
		value := flag.Arg(3)
		err = c.setKey(bucket, key, value)
	default:
		flag.Usage()
		os.Exit(1)
	}
	if err != nil {
		errf("error: %v", err)
		os.Exit(1)
	}
}

type cli struct {
	db      *bolt.DB
	verbose bool

	// sep is the bucket name separator
	sep string
}

func (c *cli) listBuckets() error {
	tx, err := c.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
		fmt.Println(string(name))
		if c.verbose {
			fmt.Println()
			stats := b.Stats()
			fmt.Printf("  Keys:  %10.d\n", stats.KeyN)
			fmt.Printf("  Depth: %10.d\n", stats.Depth)
			fmt.Println()
			fmt.Printf("  Logical Branch Pages:           %6.d\n", stats.BranchPageN)
			fmt.Printf("  Physical Branch Overflow Pages: %6.d\n", stats.BranchOverflowN)
			fmt.Printf("  Logical Leaf Pages:             %6.d\n", stats.LeafPageN)
			fmt.Printf("  Physical Leaf Overflow Pages:   %6.d\n", stats.LeafOverflowN)
			fmt.Println()
			fmt.Printf("  Bytes allocated for physical branch pages: %12.d\n", stats.BranchAlloc)
			fmt.Printf("  Bytes in-use for branch data:              %12.d\n", stats.BranchInuse)
			fmt.Printf("  Bytes allocated for physical leaf pages:   %12.d\n", stats.LeafAlloc)
			fmt.Printf("  Bytes in-use for leaf data:                %12.d\n", stats.LeafInuse)
			fmt.Println()
		}
		return nil
	})
}

// getBucket given a separator delimited name representing sub-buckets.
func (c *cli) getBucket(tx *bolt.Tx, name string) *bolt.Bucket {
	parts := strings.Split(name, c.sep)
	if len(parts) == 0 {
		return nil
	}

	b := tx.Bucket([]byte(parts[0]))
	if b == nil {
		return nil
	}

	for _, p := range parts[1:] {
		b = b.Bucket([]byte(p))
		if b == nil {
			return nil
		}
	}
	return b
}

func (c *cli) listKeys(name string) error {
	tx, err := c.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := c.getBucket(tx, name)
	if bucket == nil {
		return fmt.Errorf("bucket %q does not exist", name)
	}

	return bucket.ForEach(func(k, v []byte) error {
		if !c.verbose {
			if v != nil {
				// Skip sub-buckets
				fmt.Println(string(k))
			}
			return nil
		}

		if v == nil {
			fmt.Printf("%s (bucket)\n", k)
		} else {
			fmt.Printf("%s -> %s\n", k, v)
		}
		return nil
	})
}

func (c *cli) getKey(bucketName, keyName string) error {
	tx, err := c.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := c.getBucket(tx, bucketName)
	if bucket == nil {
		return fmt.Errorf("bucket %q does not exist", bucketName)
	}

	value := bucket.Get([]byte(keyName))
	if value == nil {
		return fmt.Errorf("key %q in bucket %q does not exist", keyName, bucketName)
	}

	n, err := os.Stdout.Write(value)
	if err != nil {
		return err
	}
	if n != len(value) {
		return fmt.Errorf("only wrote %d of %d bytes", n, len(value))
	}

	// Verbose just appends a newline
	if c.verbose {
		fmt.Println()
	}
	return nil
}

func (c *cli) setKey(bucketName, keyName, value string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		parts := strings.Split(bucketName, c.sep)
		if len(parts) == 0 {
			return fmt.Errorf("invalid bucket: %q", parts)
		}

		bucket, err := tx.CreateBucketIfNotExists([]byte(parts[0]))
		if err != nil {
			return err
		}

		for _, p := range parts[1:] {
			bucket, err = bucket.CreateBucketIfNotExists([]byte(p))
			if err != nil {
				return err
			}
		}

		return bucket.Put([]byte(keyName), []byte(value))
	})
}

func (c *cli) dumpTree() error {
	return c.displayBucket(nil, 0)
}

func (c *cli) displayBucket(bkt *bolt.Bucket, depth int) error {
	indent := strings.Repeat("  ", depth)
	buckets := []string{}
	keys := []string{}
	vals := map[string]int{}
	return c.db.View(func(tx *bolt.Tx) error {
		var cur *bolt.Cursor
		if bkt == nil {
			cur = tx.Cursor()
		} else {
			cur = bkt.Cursor()
		}
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			kstr := string(k)
			if v == nil {
				buckets = append(buckets, kstr)
			} else {
				keys = append(keys, kstr)
				vals[kstr] = len(v)
			}
		}

		sort.Strings(buckets)
		for _, k := range buckets {
			fmt.Printf("%s* %s\n", indent, k)
			var child *bolt.Bucket
			if bkt == nil {
				child = tx.Bucket([]byte(k))
			} else {
				child = bkt.Bucket([]byte(k))
			}
			if err := c.displayBucket(child, depth+1); err != nil {
				return err
			}
		}

		for _, k := range keys {
			fmt.Printf("%s - %s (%d bytes)\n", indent, k, vals[k])
		}
		return nil
	})
}

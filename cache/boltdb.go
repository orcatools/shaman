package cache

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net/url"

	bolt "github.com/etcd-io/bbolt"
	"github.com/orcatools/shaman/config"
	shaman "github.com/orcatools/shaman/core/common"
)

const (
	// name of the bucket to use within boltdb
	// NOTE: should this be a config option?
	dnsbucket = "dns"
)

type boltDb struct {
	db *bolt.DB
}

func (client *boltDb) initialize() error {
	// fmt.Println("initializing boltdb")
	// fmt.Println("config", config.L2Connect)
	u, err := url.Parse(config.L2Connect)

	db, err := bolt.Open(u.Path, 0600, nil)
	if err != nil {
		return err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(dnsbucket))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	client.db = db
	return nil
}

func (client boltDb) addRecord(resource shaman.Resource) error {
	return client.updateRecord(resource.Domain, resource)
}

func (client boltDb) getRecord(domain string) (*shaman.Resource, error) {
	var result shaman.Resource
	err := client.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(dnsbucket))
		kvPair := b.Get([]byte(addPrefix(domain)))
		if kvPair == nil {
			return errNoRecordError
		}
		err := gob.NewDecoder(bytes.NewReader(kvPair)).Decode(&result)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (client boltDb) updateRecord(domain string, resource shaman.Resource) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(&resource)
	if err != nil {
		return err
	}

	err = client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(dnsbucket))
		err := b.Put([]byte(addPrefix(domain)), buf.Bytes())
		return err
	})
	return err
}

func (client boltDb) deleteRecord(domain string) error {
	err := client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(dnsbucket))
		return b.Delete([]byte(addPrefix(domain)))
	})
	return err
}

func (client boltDb) resetRecords(resources []shaman.Resource) error {
	err := client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(dnsbucket))
		return b.DeleteBucket([]byte(dnsbucket))
	})
	if err != nil {
		return err
	}
	for i := range resources {
		err = client.addRecord(resources[i]) // prevents duplicates
		if err != nil {
			return fmt.Errorf("Failed to save records - %v", err)
		}
	}
	return nil
}

func (client boltDb) listRecords() ([]shaman.Resource, error) {
	result := []shaman.Resource{}
	err := client.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(dnsbucket))
		b.ForEach(func(k, v []byte) error {
			var resource shaman.Resource
			err := gob.NewDecoder(bytes.NewReader(v)).Decode(&resource)
			if err != nil {
				return err
			}
			result = append(result, resource)
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

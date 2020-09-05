// Package cache provides a pluggable backend for persistent record storage.
package cache

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/orcatools/shaman/config"
	shaman "github.com/orcatools/shaman/core/common"
)

var (
	storage          cacher
	errNoRecordError = errors.New("No Record Found")
)

// The cacher interface is what all the backends [will] implement
type cacher interface {
	initialize() error
	addRecord(resource shaman.Resource) error
	getRecord(domain string) (*shaman.Resource, error)
	updateRecord(domain string, resource shaman.Resource) error
	deleteRecord(domain string) error
	resetRecords(resources []shaman.Resource) error
	listRecords() ([]shaman.Resource, error)
}

// Initialize sets default cacher and initialize it
func Initialize() error {
	u, err := url.Parse(config.L2Connect)
	if err != nil {
		return fmt.Errorf("Failed to parse 'l2-connect' - %v", err)
	}

	switch u.Scheme {
	case "scribble":
		config.Log.Debug("using scribble storage")
		storage = &scribbleDb{}
	case "postgres":
		config.Log.Debug("using postgresql storage")
		storage = &postgresDb{}
	case "postgresql":
		config.Log.Debug("using postgresql storage")
		storage = &postgresDb{}
	case "consul":
		config.Log.Debug("using consul storage")
		storage = &consulDb{}
	case "boltdb":
		config.Log.Debug("using boltdb storage")
		storage = &boltDb{}
	case "none":
		config.Log.Debug("not using storage")
		storage = nil
	default:
		config.Log.Debug("using default storage")
		storage = &scribbleDb{}
	}

	if storage != nil {
		err = storage.initialize()
		if err != nil {
			storage = nil
			config.Log.Info("Failed to initialize cache, turning off - %v", err)
			err = nil
		}
	}

	return err
}

// AddRecord adds a record to the persistent cache
func AddRecord(resource *shaman.Resource) error {
	if storage == nil {
		return nil
	}
	resource.Validate()
	return storage.addRecord(*resource)
}

// GetRecord gets a record to the persistent cache
func GetRecord(domain string) (*shaman.Resource, error) {
	if storage == nil {
		return nil, nil
	}

	shaman.SanitizeDomain(&domain)
	return storage.getRecord(domain)
}

// UpdateRecord updates a record in the persistent cache
func UpdateRecord(domain string, resource *shaman.Resource) error {
	if storage == nil {
		return nil
	}
	shaman.SanitizeDomain(&domain)
	resource.Validate()
	return storage.updateRecord(domain, *resource)
}

// DeleteRecord removes a record from the persistent cache
func DeleteRecord(domain string) error {
	if storage == nil {
		return nil
	}
	shaman.SanitizeDomain(&domain)
	return storage.deleteRecord(domain)
}

// ResetRecords replaces all records in the persistent cache
func ResetRecords(resources *[]shaman.Resource) error {
	if storage == nil {
		return nil
	}
	for i := range *resources {
		(*resources)[i].Validate()
	}

	return storage.resetRecords(*resources)
}

// ListRecords lists all records in the persistent cache
func ListRecords() ([]shaman.Resource, error) {
	if storage == nil {
		return make([]shaman.Resource, 0), nil
	}
	return storage.listRecords()
}

// Exists returns whether the default cacher exists
func Exists() bool {
	return storage != nil
}

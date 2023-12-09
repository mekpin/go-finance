package model

import (
	"time"

	"github.com/rs/xid"
)

// Cart represents a model for a shopping cart.
type Cart struct {
	Id          xid.ID    `json:"id"`
	NamaItem    string    `json:"nama_item"`
	Jumlah      int       `json:"jumlah"`
	HargaSatuan int       `json:"harga_satuan"`
	HargaTotal  int       `json:"harga_total"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	DeletedAt   time.Time `json:"deleted_at"`
}

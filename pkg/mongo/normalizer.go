package mongo

import "github.com/theobitoproject/kankuro/pkg/protocol"

// Normalizer defines a type of normalization for data
type Normalizer interface {
	// Marshal takes an Airbyte record and
	// and returns the respective document
	Marshal(rec *protocol.Record) (*document, error)
}

func newNormalizer(
	cc *protocol.ConfiguredCatalog,
	enableBasicNormalization bool,
) Normalizer {
	if enableBasicNormalization {
		return newBasicNormalizer(cc)
	}

	return newRawNormalizer()
}

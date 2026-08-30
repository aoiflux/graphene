package main

// The registry.
//
// Every command in the tool is here, once. This slice is what dispatch walks,
// what help is printed from, and what shell completion is generated from — so
// the three cannot drift apart the way the dispatch switch, the usage text and
// the package doc comment had.
//
// The noun-verb spelling is the primary one and the flat spelling that predates
// it is an alias: kept working, hidden from help, and never removed.
// `graphene custody <dir>` and `graphene provenance custody <dir>` are one
// command reached two ways, and a script written against this tool before
// groups existed keeps running unchanged.

// registry is the whole command surface, in the order help prints it.
var registry = []*Command{
	// The image, the log, and what they say about each other.
	storeInfo,
	storeCSR,
	storeMigrate,

	// The write-ahead log.
	walShow,

	// Where an entity came from, and who has vouched for it.
	provenanceCustody,
	provenanceExport,
	provenanceVerify,

	// Checkpoints, and the external roots they are published to.
	anchorVerify,

	// The governance ledgers.
	redactionList,
	grantList,

	// Consistent copies.
	backupCreate,
	backupVerify,

	// Bulk transfer.
	exportGraph,
	importGraph,

	// Verification.
	debugIndexes,

	// Top level.
	versionCmd,
}

var versionCmd = plain(Command{
	Name: "version", Short: "what this binary is, and what it writes",
	Open: OpenNone,
}, runVersion)

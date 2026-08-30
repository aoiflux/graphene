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
//
// The order below is the order help prints, and it is a reading order rather
// than an alphabetical one: what the store is, then what is in it, then what
// its shape is, then what can be proved about it, then what can be copied out
// of it. Alphabetical would put `anchor` first and `wal` last, which is the
// order of the alphabet and not of anybody's investigation.

// registry is the whole command surface.
var registry = []*Command{
	// The image, the log, and what they say about each other.
	storeInfo,
	storeCSR,
	storeStats,
	storeSnapshot,
	storeHealth,
	storeMigrate,

	// The write-ahead log.
	walShow,
	walSegments,
	walVerify,
	walCompact,

	// What is in the store.
	nodeGet,
	nodeList,
	nodeCount,
	nodeDegree,
	nodeNeighbours,
	nodeExplain,
	nodeVerify,
	nodeCreate,
	nodeUpsert,
	nodeDelete,

	edgeGet,
	edgeList,
	edgeCount,
	edgeOf,
	edgeExplain,
	edgeVerify,
	edgeProvenance,
	edgeCreate,
	edgeUpsert,
	edgeDelete,

	// What shape it is.
	traverseBFS,
	traverseDFS,
	traversePath,
	traverseCycle,
	traverseSubgraph,
	traversePattern,

	queryRelations,

	// Where an entity came from, and who has vouched for it.
	provenanceChain,
	provenanceCustody,
	provenanceExport,
	provenanceVerify,

	// Checkpoints, and the external roots they are published to.
	anchorList,
	anchorShow,
	anchorAdd,
	anchorVerify,

	// Signing keys.
	keysTimeline,

	// Attestations and the audit chain, under the name somebody looks for.
	assertionAdd,
	assertionList,
	assertionVerify,

	// The governance ledgers.
	redactionList,
	redactionVerify,
	redactionImpact,
	redactionTombstones,
	redactionApply,

	grantList,
	grantVerify,
	grantCheck,
	grantAdd,
	grantRevoke,

	// Consistent copies.
	backupCreate,
	backupVerify,
	backupRestore,

	// Bulk transfer.
	exportGraph,
	exportSubgraph,
	exportViz,
	exportBundle,
	importGraph,

	// Verification.
	debugHashCheck,
	debugSignatureCheck,
	debugIndexes,
	debugUnique,
	debugStats,
	debugIntegrity,

	// Maintenance, and the two refusals that keep the doctrine legible.
	maintenanceCompact,
	maintenanceReindex,
	maintenanceRepair,
	maintenanceVacuum,

	// The user's own config, and the named stores in it.
	configPathCmd,
	configShow,
	configInit,

	profileList,
	profileAdd,
	profileRemove,
	profileUse,

	// Top level.
	completionCmd,
	versionCmd,
}

var versionCmd = plain(Command{
	Name: "version", Short: "what this binary is, and what it writes",
	Open: OpenNone,
}, runVersion)

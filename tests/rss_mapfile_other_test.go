//go:build !linux && !windows

package graphene_test

import "runtime"

const runtimeGOOS = runtime.GOOS

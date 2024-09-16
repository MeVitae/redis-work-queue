#!/bin/bash
cd "$(realpath "$(dirname $0)")"
exec go run . "$@"

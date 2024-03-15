#!/bin/bash
cd "$(realpath "$(dirname $0)")"/Cleaner
exec dotnet run -v quiet "$@"

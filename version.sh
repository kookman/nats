#!/bin/sh

printf %s ${DUB_PACKAGE}_$(git describe --dirty --always --tags)

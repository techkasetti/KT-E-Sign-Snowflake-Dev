#!/usr/bin/env bash
# Final aggregator script to register everything end-to-end
set -e
./sql/register/register_all_remaining.sh
./sql/register/register_evidence_zipper.sh
./sql/register/register_ingest_proc.sh
./sql/register/register_assembly_proc.sh
./sql/register/register_hsm_stub.sh
./register/register_faiss_externalfn.sh
echo "Final registration sequence complete."

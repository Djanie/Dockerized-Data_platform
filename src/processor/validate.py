# --- import your validate function near other imports ---
# at top of file
from src.processor.validate import validate_orders

# --- after writing file to disk (out_path) ---
# df is still available (the generated DataFrame)
# Convert to same validation semantics: use your validate_orders function
is_valid, validation_msg = validate_orders(str(out_path))

# Build a lightweight validation summary for file_registry
rows_in = len(df)
duplicate_count = int(df.duplicated(keep=False).sum())
validation_summary = {
    "rows_in": rows_in,
    "duplicate_count": duplicate_count,
    "validator_passed": bool(is_valid),
    "validator_message": validation_msg
}

# If Postgres available, insert a registry row. (This matches the earlier PgRegistry usage)
record_id = None
if psycopg2 is not None:
    try:
        pg = PgRegistry(cfg)
        pg.ensure_table()
        if not is_valid:
            # validation failed: register and stop early
            record_id = pg.insert(out_path.name, datetime.utcnow(), "validation_failed", rows_in, duplicate_count, validation_summary)
            logging.error("Validation failed for %s: %s", out_path, validation_msg)
            # stop here — do not upload or call upsert
            return 0
        else:
            # validation passed -> register as uploaded (we will upload next)
            record_id = pg.insert(out_path.name, datetime.utcnow(), "uploaded", rows_in, duplicate_count, validation_summary)
    except Exception as e:
        logging.warning("Could not write file_registry: %s", e)
else:
    logging.info("psycopg2 not installed; skipping file_registry insert")

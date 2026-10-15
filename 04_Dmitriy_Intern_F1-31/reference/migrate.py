from pathlib import Path

import postgres_store


def main():
    migration = Path(__file__).with_name("migrations") / "002_sales_manager.sql"
    sql = migration.read_text(encoding="utf-8")
    with postgres_store.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    print("MIGRATION_002_SALES_MANAGER_OK", flush=True)


if __name__ == "__main__":
    main()

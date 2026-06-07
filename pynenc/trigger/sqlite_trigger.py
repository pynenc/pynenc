"""
SQLite-based implementation of the Pynenc trigger subsystem.

This module provides a trigger system implementation that stores
all its state in a SQLite database. Suitable for cross-process coordination and testing.
"""

from collections.abc import Iterable
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING

from pynenc.conf.config_trigger import ConfigTriggerSQLite
from pynenc.identifiers.task_id import TaskId
from pynenc.models.trigger_definition_dto import TriggerDefinitionDTO
from pynenc.trigger.base_trigger import BaseTrigger
from pynenc.trigger.conditions import (
    ConditionContext,
    TriggerCondition,
    ValidCondition,
    CompositeLogic,
)
from pynenc.trigger.monitoring import (
    EventMarker,
    EventMarkerPage,
    EventRecord,
    TriggerRunRecord,
)
from pynenc.util.sqlite_utils import create_sqlite_connection as sqlite_conn
from pynenc.util.sqlite_utils import TableNames, delete_tables_with_prefix

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.util.sqlite_utils import SQLiteConnection


# ---------------------------------------------------------------------- #
# Table-name registry
# ---------------------------------------------------------------------- #
# SQLiteTrigger uses per-app table prefixes; keeping the names in one object
# makes SQL construction local while preserving app-level isolation.


class Tables(TableNames):
    """Table names for trigger, scoped by app_id."""

    def __init__(self, app_id: str) -> None:
        super().__init__(app_id, "trg")
        p = self.table_prefix
        self.CONDITIONS = f"{p}_conditions"
        self.TRIGGERS = f"{p}_triggers"
        self.CONDITION_TRIGGERS = f"{p}_condition_triggers"
        self.VALID_CONDITIONS = f"{p}_valid_conditions"
        self.SOURCE_TASK_CONDITIONS = f"{p}_source_task_conditions"
        self.EXECUTION_CLAIMS = f"{p}_execution_claims"
        self.TRIGGER_RUN_CLAIMS = f"{p}_trigger_run_claims"
        self.EVENTS = f"{p}_events"
        self.EVENT_CONDITIONS = f"{p}_event_conditions"
        self.EVENT_TRIGGERED_INVOCATIONS = f"{p}_event_triggered_invocations"
        self.TRIGGER_RUNS = f"{p}_trigger_runs"
        self.TRIGGER_RUN_CONDITIONS = f"{p}_trigger_run_conditions"
        self.SCHEMA_VERSION = f"{p}_schema_version"


class SQLiteTrigger(BaseTrigger):
    """
    SQLite-based implementation of the Pynenc trigger system.

    Stores all trigger, condition, and claim data in a SQLite database for cross-process safety.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.tables = Tables(app.app_id)
        self.sqlite_db_path = self.conf.sqlite_db_path
        self._init_tables()

    @cached_property
    def conf(self) -> ConfigTriggerSQLite:
        return ConfigTriggerSQLite(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    # ------------------------------------------------------------------ #
    # Schema initialization
    # ------------------------------------------------------------------ #
    # Table creation stays in this class for now so the large-file refactor
    # remains comment-only; the boundary is explicit for a later extraction.

    def _init_tables(self) -> None:
        """Initialize SQLite tables for trigger state."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.tables.CONDITIONS} (
                    condition_id TEXT PRIMARY KEY,
                    condition_json TEXT NOT NULL,
                    last_cron_execution TIMESTAMP
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.tables.TRIGGERS} (
                    trigger_id TEXT PRIMARY KEY,
                    task_id_key TEXT NOT NULL,
                    logic_value TEXT NOT NULL,
                    argument_provider_json TEXT
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.tables.CONDITION_TRIGGERS} (
                    condition_id TEXT NOT NULL,
                    trigger_id TEXT NOT NULL,
                    PRIMARY KEY (condition_id, trigger_id)
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.tables.VALID_CONDITIONS} (
                    valid_condition_id TEXT PRIMARY KEY,
                    valid_condition_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.tables.SOURCE_TASK_CONDITIONS} (
                    task_id_key TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    PRIMARY KEY (task_id_key, condition_id)
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.tables.EXECUTION_CLAIMS} (
                    claim_key TEXT PRIMARY KEY,
                    expiration TIMESTAMP
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.tables.TRIGGER_RUN_CLAIMS} (
                    trigger_run_id TEXT PRIMARY KEY,
                    expiration TIMESTAMP
                )
                """
            )
            self._init_monitoring_tables(conn)
            self._ensure_schema_version(conn)
            conn.commit()

    # ------------------------------------------------------------------ #
    # Schema versioning
    # ------------------------------------------------------------------ #

    SCHEMA_VERSION_CURRENT: int = 3

    def _ensure_schema_version(self, conn: "SQLiteConnection") -> None:
        """Create the schema-version table and record the current version.

        Used to detect future column or index migrations. A missing version
        means the schema predates this introduction; ``store_event`` and
        related operations remain backward compatible at version 1.
        """
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.tables.SCHEMA_VERSION} (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            INSERT INTO {self.tables.SCHEMA_VERSION} (key, value)
            VALUES ('version', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (self.SCHEMA_VERSION_CURRENT,),
        )

    def _init_monitoring_tables(self, conn: "SQLiteConnection") -> None:
        """Create event/trigger-run monitoring tables and indexes."""
        self._init_event_tables(conn)
        self._init_event_invocation_index(conn)
        self._init_trigger_run_tables(conn)

    def _init_event_tables(self, conn: "SQLiteConnection") -> None:
        """Create the events table plus its core indexes."""
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.tables.EVENTS} (
                event_id TEXT PRIMARY KEY,
                event_code TEXT NOT NULL,
                event_timestamp REAL NOT NULL,
                event_json TEXT NOT NULL,
                matched_condition_count INTEGER NOT NULL DEFAULT 0,
                triggered_invocation_count INTEGER NOT NULL DEFAULT 0,
                emitted_by_invocation_id TEXT,
                emitted_by_task_id TEXT,
                emitted_by_runner_context_id TEXT
            )
            """
        )
        columns = {
            row[1]
            for row in conn.execute(
                f"PRAGMA table_info({self.tables.EVENTS})"
            ).fetchall()
        }
        if "emitted_by_runner_context_id" not in columns:
            conn.execute(
                f"ALTER TABLE {self.tables.EVENTS} "
                "ADD COLUMN emitted_by_runner_context_id TEXT"
            )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.tables.EVENTS}_code_time "
            f"ON {self.tables.EVENTS}(event_code, event_timestamp)"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.tables.EVENTS}_time "
            f"ON {self.tables.EVENTS}(event_timestamp)"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.tables.EVENTS}_emit_inv "
            f"ON {self.tables.EVENTS}(emitted_by_invocation_id) "
            f"WHERE emitted_by_invocation_id IS NOT NULL"
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.tables.EVENT_CONDITIONS} (
                event_id TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                valid_condition_id TEXT,
                matched INTEGER NOT NULL,
                PRIMARY KEY (event_id, condition_id)
            )
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.tables.EVENT_CONDITIONS}_cond "
            f"ON {self.tables.EVENT_CONDITIONS}(condition_id)"
        )

    def _init_event_invocation_index(self, conn: "SQLiteConnection") -> None:
        """Create the relation table linking events to triggered invocations."""
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.tables.EVENT_TRIGGERED_INVOCATIONS} (
                event_id TEXT NOT NULL,
                invocation_id TEXT NOT NULL,
                trigger_run_id TEXT,
                linked_at REAL NOT NULL,
                PRIMARY KEY (event_id, invocation_id)
            )
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.tables.EVENT_TRIGGERED_INVOCATIONS}_inv "
            f"ON {self.tables.EVENT_TRIGGERED_INVOCATIONS}(invocation_id)"
        )

    def _init_trigger_run_tables(self, conn: "SQLiteConnection") -> None:
        """Create trigger-run tables, indexes, and participant rows."""
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.tables.TRIGGER_RUNS} (
                trigger_run_id TEXT PRIMARY KEY,
                trigger_id TEXT NOT NULL,
                task_id_key TEXT NOT NULL,
                logic_value TEXT NOT NULL,
                claimed_at REAL,
                executed_at REAL,
                triggered_invocation_id TEXT,
                trigger_run_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.tables.TRIGGER_RUNS}_inv "
            f"ON {self.tables.TRIGGER_RUNS}(triggered_invocation_id)"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.tables.TRIGGER_RUNS}_task_time "
            f"ON {self.tables.TRIGGER_RUNS}(task_id_key, executed_at)"
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.tables.TRIGGER_RUN_CONDITIONS} (
                trigger_run_id TEXT NOT NULL,
                context_type TEXT,
                condition_id TEXT,
                valid_condition_id TEXT,
                event_id TEXT,
                source_invocation_id TEXT
            )
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.tables.TRIGGER_RUN_CONDITIONS}_run "
            f"ON {self.tables.TRIGGER_RUN_CONDITIONS}(trigger_run_id)"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.tables.TRIGGER_RUN_CONDITIONS}_evt "
            f"ON {self.tables.TRIGGER_RUN_CONDITIONS}(event_id)"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.tables.TRIGGER_RUN_CONDITIONS}_src_inv "
            f"ON {self.tables.TRIGGER_RUN_CONDITIONS}(source_invocation_id)"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.tables.TRIGGER_RUN_CONDITIONS}_valid "
            f"ON {self.tables.TRIGGER_RUN_CONDITIONS}(valid_condition_id)"
        )

    # ------------------------------------------------------------------ #
    # Registration storage
    # ------------------------------------------------------------------ #
    # These methods persist trigger definitions, condition definitions, and
    # reverse indexes used by the BaseTrigger lifecycle-report hooks.

    def _register_condition(self, condition: TriggerCondition) -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {self.tables.CONDITIONS} (condition_id, condition_json) VALUES (?, ?)",
                (condition.condition_id, condition.to_json(self.app)),
            )
            conn.commit()

    def get_condition(self, condition_id: str) -> TriggerCondition | None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT condition_json FROM {self.tables.CONDITIONS} WHERE condition_id = ?",
                (condition_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if row:
                return TriggerCondition.from_json(row[0], self.app)
            return None

    def register_trigger(self, trigger: "TriggerDefinitionDTO") -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {self.tables.TRIGGERS} (trigger_id, task_id_key, logic_value, argument_provider_json) VALUES (?, ?, ?, ?)",
                (
                    trigger.trigger_id,
                    trigger.task_id.key,
                    trigger.logic.value,
                    trigger.argument_provider_json,
                ),
            )
            for condition_id in trigger.condition_ids:
                conn.execute(
                    f"INSERT OR REPLACE INTO {self.tables.CONDITION_TRIGGERS} (condition_id, trigger_id) VALUES (?, ?)",
                    (condition_id, trigger.trigger_id),
                )
            conn.commit()

    def _get_trigger(self, trigger_id: str) -> "TriggerDefinitionDTO | None":
        results = self._get_triggers([trigger_id])
        return results[0] if results else None

    def _get_triggers(self, trigger_ids: list[str]) -> list["TriggerDefinitionDTO"]:
        """Fetch multiple triggers in batch using two queries.

        Returns a list of TriggerDefinitionDTO for the provided trigger_ids
        preserving any that exist in the DB.
        """
        if not trigger_ids:
            return []

        trg_ids_query = ",".join(["?" for _ in trigger_ids])
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT trigger_id, task_id_key, logic_value, argument_provider_json FROM {self.tables.TRIGGERS} WHERE trigger_id IN ({trg_ids_query})",
                tuple(trigger_ids),
            )
            triggers_rows = cursor.fetchall()
            cursor.close()
            if not triggers_rows:
                return []

            # Fetch condition ids for all triggers in one query
            cursor = conn.execute(
                f"SELECT condition_id, trigger_id FROM {self.tables.CONDITION_TRIGGERS} WHERE trigger_id IN ({trg_ids_query})",
                tuple(trigger_ids),
            )
            condition_rows = cursor.fetchall()
            cursor.close()

            # Build mapping trigger_id -> list[condition_id]
            condition_map: dict[str, list[str]] = defaultdict(list)
            for condition_id, tid in condition_rows:
                condition_map[tid].append(condition_id)

            # Construct DTOs preserving only found triggers
            dto_list: list[TriggerDefinitionDTO] = []
            for tid, task_id_key, logic_value, argument_provider_json in triggers_rows:
                dto_list.append(
                    TriggerDefinitionDTO(
                        trigger_id=tid,
                        task_id=TaskId.from_key(task_id_key),
                        condition_ids=condition_map[tid],
                        logic=CompositeLogic(logic_value),
                        argument_provider_json=argument_provider_json,
                    )
                )

            return dto_list

    def get_triggers_for_condition(
        self, condition_id: str
    ) -> list["TriggerDefinitionDTO"]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT trigger_id FROM {self.tables.CONDITION_TRIGGERS} WHERE condition_id = ?",
                (condition_id,),
            )
            trigger_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            # Fetch all triggers in batch
            return self._get_triggers(trigger_ids)

    # ------------------------------------------------------------------ #
    # Valid-condition storage
    # ------------------------------------------------------------------ #
    # Valid conditions are transient loop inputs. SQLite persists them so
    # multiple runners can coordinate trigger execution across processes.

    def record_valid_condition(self, valid_condition: ValidCondition) -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {self.tables.VALID_CONDITIONS} (valid_condition_id, valid_condition_json) VALUES (?, ?)",
                (valid_condition.valid_condition_id, valid_condition.to_json(self.app)),
            )
            conn.commit()

    def record_valid_conditions(self, valid_conditions: list[ValidCondition]) -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            for valid_condition in valid_conditions:
                conn.execute(
                    f"INSERT OR REPLACE INTO {self.tables.VALID_CONDITIONS} (valid_condition_id, valid_condition_json) VALUES (?, ?)",
                    (
                        valid_condition.valid_condition_id,
                        valid_condition.to_json(self.app),
                    ),
                )
            conn.commit()

    def get_valid_conditions(self) -> dict[str, ValidCondition]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT valid_condition_id, valid_condition_json FROM {self.tables.VALID_CONDITIONS}"
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            return {
                row[0]: ValidCondition.from_json(row[1], self.app)
                for row in cursor_rows
            }

    def clear_valid_conditions(self, conditions: Iterable[ValidCondition]) -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            for condition in conditions:
                conn.execute(
                    f"DELETE FROM {self.tables.VALID_CONDITIONS} WHERE valid_condition_id = ?",
                    (condition.valid_condition_id,),
                )
            conn.commit()

    def _get_all_conditions(self) -> list[TriggerCondition]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT condition_json FROM {self.tables.CONDITIONS}"
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            return [TriggerCondition.from_json(row[0], self.app) for row in cursor_rows]

    # ------------------------------------------------------------------ #
    # Cron scheduling state
    # ------------------------------------------------------------------ #
    # The last-execution column doubles as an optimistic claim guard so cron
    # conditions do not fire twice when multiple runners poll at once.

    def get_last_cron_execution(self, condition_id: str) -> datetime | None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT last_cron_execution FROM {self.tables.CONDITIONS} WHERE condition_id = ?",
                (condition_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if row and row[0]:
                return datetime.fromisoformat(row[0])
            return None

    def store_last_cron_execution(
        self,
        condition_id: str,
        execution_time: datetime,
        expected_last_execution: datetime | None = None,
    ) -> bool:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT last_cron_execution FROM {self.tables.CONDITIONS} WHERE condition_id = ?",
                (condition_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            current = datetime.fromisoformat(row[0]) if row and row[0] else None
            if (
                expected_last_execution is not None
                and current != expected_last_execution
            ):
                return False
            conn.execute(
                f"UPDATE {self.tables.CONDITIONS} SET last_cron_execution = ? WHERE condition_id = ?",
                (execution_time.isoformat(), condition_id),
            )
            conn.commit()
            return True

    # ------------------------------------------------------------------ #
    # Source-task indexes and trigger-run claims
    # ------------------------------------------------------------------ #
    # Source-task indexes answer lifecycle-report queries; claim tables are
    # short-lived locks that protect trigger execution across processes.

    def _register_source_task_condition(
        self, task_id: "TaskId", condition_id: str
    ) -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {self.tables.SOURCE_TASK_CONDITIONS} (task_id_key, condition_id) VALUES (?, ?)",
                (task_id.key, condition_id),
            )
            conn.commit()

    def get_conditions_sourced_from_task(
        self, task_id: "TaskId", context_type: type[ConditionContext] | None = None
    ) -> list[TriggerCondition]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT condition_id FROM {self.tables.SOURCE_TASK_CONDITIONS} WHERE task_id_key = ?",
                (task_id.key,),
            )
            condition_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conditions = [self.get_condition(cid) for cid in condition_ids]
            conditions = [c for c in conditions if c]
            if context_type is not None:
                conditions = [
                    cond for cond in conditions if cond.context_type == context_type
                ]
            return conditions

    def claim_trigger_execution(
        self, trigger_id: str, valid_condition_id: str, expiration_seconds: int = 60
    ) -> bool:
        claim_key = f"{trigger_id}:{valid_condition_id}"
        now = datetime.now(UTC)
        expiration = now + timedelta(seconds=expiration_seconds)
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT expiration FROM {self.tables.EXECUTION_CLAIMS} WHERE claim_key = ?",
                (claim_key,),
            )
            row = cursor.fetchone()
            cursor.close()
            if row and row[0]:
                existing_expiration = datetime.fromisoformat(row[0])
                if existing_expiration > now:
                    return False
            conn.execute(
                f"INSERT OR REPLACE INTO {self.tables.EXECUTION_CLAIMS} (claim_key, expiration) VALUES (?, ?)",
                (claim_key, expiration.isoformat()),
            )
            conn.commit()
            return True

    def claim_trigger_run(
        self, trigger_run_id: str, expiration_seconds: int = 60
    ) -> bool:
        now = datetime.now(UTC)
        expiration = now + timedelta(seconds=expiration_seconds)
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT expiration FROM {self.tables.TRIGGER_RUN_CLAIMS} WHERE trigger_run_id = ?",
                (trigger_run_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if row and row[0]:
                existing_expiration = datetime.fromisoformat(row[0])
                if existing_expiration > now:
                    return False
            conn.execute(
                f"INSERT OR REPLACE INTO {self.tables.TRIGGER_RUN_CLAIMS} (trigger_run_id, expiration) VALUES (?, ?)",
                (trigger_run_id, expiration.isoformat()),
            )
            conn.commit()
            return True

    # ------------------------------------------------------------------ #
    # Task trigger cleanup and full purge
    # ------------------------------------------------------------------ #
    # Task cleanup removes definitions for one task. Full purge drops all
    # trigger tables for this app prefix and recreates the schema.

    def clean_task_trigger_definitions(self, task_id: "TaskId") -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT trigger_id FROM {self.tables.TRIGGERS} WHERE task_id_key = ?",
                (task_id.key,),
            )
            trigger_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            for trigger_id in trigger_ids:
                conn.execute(
                    f"DELETE FROM {self.tables.TRIGGERS} WHERE trigger_id = ?",
                    (trigger_id,),
                )
                conn.execute(
                    f"DELETE FROM {self.tables.CONDITION_TRIGGERS} WHERE trigger_id = ?",
                    (trigger_id,),
                )
            conn.commit()

    def _purge(self) -> None:
        delete_tables_with_prefix(self.sqlite_db_path, self.tables.table_prefix)
        self._init_tables()

    # ------------------------------------------------------------------ #
    # Monitoring API
    # ------------------------------------------------------------------ #

    # Event records and event indexes. Event JSON is stored as the canonical
    # payload while relation tables provide indexed reads for Pynmon.

    def store_event(self, event: EventRecord) -> None:
        """Upsert an event row plus its per-condition match links.

        Legacy callers may populate ``event.triggered_invocation_ids``
        directly; in that case the relation table is also seeded so that
        the indexed read APIs see the same data.
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                INSERT OR REPLACE INTO {self.tables.EVENTS} (
                    event_id, event_code, event_timestamp, event_json,
                    matched_condition_count, triggered_invocation_count,
                    emitted_by_invocation_id, emitted_by_task_id,
                    emitted_by_runner_context_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.event_code,
                    event.timestamp.timestamp(),
                    event.to_json(self.app),
                    len(event.matched_condition_ids),
                    len(event.triggered_invocation_ids),
                    event.emitted_by_invocation_id,
                    event.emitted_by_task_id,
                    event.emitted_by_runner_context_id,
                ),
            )
            conn.execute(
                f"DELETE FROM {self.tables.EVENT_CONDITIONS} WHERE event_id = ?",
                (event.event_id,),
            )
            valid_ids = list(event.valid_condition_ids)
            for idx, cond_id in enumerate(event.matched_condition_ids):
                vc_id = valid_ids[idx] if idx < len(valid_ids) else None
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO {self.tables.EVENT_CONDITIONS} (
                        event_id, condition_id, valid_condition_id, matched
                    ) VALUES (?, ?, ?, 1)
                    """,
                    (event.event_id, cond_id, vc_id),
                )
            if event.triggered_invocation_ids:
                linked_at = datetime.now(UTC).timestamp()
                for inv_id in event.triggered_invocation_ids:
                    conn.execute(
                        f"""
                        INSERT OR IGNORE INTO {self.tables.EVENT_TRIGGERED_INVOCATIONS}
                            (event_id, invocation_id, trigger_run_id, linked_at)
                        VALUES (?, ?, NULL, ?)
                        """,
                        (event.event_id, inv_id, linked_at),
                    )
            conn.commit()

    def get_event(self, event_id: str) -> EventRecord | None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            row = conn.execute(
                f"SELECT event_json FROM {self.tables.EVENTS} WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if not row:
                return None
            inv_rows = conn.execute(
                f"SELECT invocation_id FROM {self.tables.EVENT_TRIGGERED_INVOCATIONS} "
                f"WHERE event_id = ? ORDER BY linked_at",
                (event_id,),
            ).fetchall()
        record = EventRecord.from_json(row[0], self.app)
        record.triggered_invocation_ids = [r[0] for r in inv_rows]
        return record

    def get_events(
        self,
        *,
        event_code: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        matched: bool | None = None,
        triggered: bool | None = None,
        emitted_by_invocation_id: str | None = None,
        emitted_by_task_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[EventRecord]:
        where, params = self._build_event_where(
            event_code,
            start_time,
            end_time,
            matched,
            triggered,
            emitted_by_invocation_id,
            emitted_by_task_id,
        )
        sql = (
            f"SELECT event_id, event_json FROM {self.tables.EVENTS} {where} "
            f"ORDER BY event_timestamp DESC LIMIT ? OFFSET ?"
        )
        with sqlite_conn(self.sqlite_db_path) as conn:
            rows = conn.execute(sql, (*params, limit, offset)).fetchall()
            event_ids = [row[0] for row in rows]
            invocations = self._load_invocations_for_events(conn, event_ids)
        records = []
        for row in rows:
            rec = EventRecord.from_json(row[1], self.app)
            rec.triggered_invocation_ids = invocations.get(rec.event_id, [])
            records.append(rec)
        return records

    def count_events(
        self,
        *,
        event_code: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        matched: bool | None = None,
        triggered: bool | None = None,
        emitted_by_invocation_id: str | None = None,
        emitted_by_task_id: str | None = None,
    ) -> int:
        where, params = self._build_event_where(
            event_code,
            start_time,
            end_time,
            matched,
            triggered,
            emitted_by_invocation_id,
            emitted_by_task_id,
        )
        with sqlite_conn(self.sqlite_db_path) as conn:
            row = conn.execute(
                f"SELECT COUNT(*) FROM {self.tables.EVENTS} {where}", params
            ).fetchone()
        return int(row[0]) if row else 0

    def get_events_emitted_by_invocation(
        self,
        invocation_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
    ) -> list[EventRecord]:
        """Override: direct indexed SELECT on ``emitted_by_invocation_id``."""
        return self.get_events(
            emitted_by_invocation_id=invocation_id,
            limit=limit,
            offset=offset,
        )

    def get_event_markers_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        *,
        event_code: str | None = None,
        state: str = "all",
        limit: int = 1000,
        offset: int = 0,
    ) -> EventMarkerPage:
        clauses = ["event_timestamp >= ?", "event_timestamp <= ?"]
        params: list = [start_time.timestamp(), end_time.timestamp()]
        if event_code is not None:
            clauses.append("event_code = ?")
            params.append(event_code)
        if state == "matched":
            clauses.append("matched_condition_count > 0")
        elif state == "unmatched":
            clauses.append("matched_condition_count = 0")
        elif state == "triggered":
            clauses.append("triggered_invocation_count > 0")
        elif state == "untriggered":
            clauses.append("triggered_invocation_count = 0")
        where = " AND ".join(clauses)
        with sqlite_conn(self.sqlite_db_path) as conn:
            total_row = conn.execute(
                f"SELECT COUNT(*) FROM {self.tables.EVENTS} WHERE {where}",
                tuple(params),
            ).fetchone()
            total = int(total_row[0]) if total_row else 0
            rows = conn.execute(
                f"""
                SELECT event_id, event_code, event_timestamp,
                       matched_condition_count, triggered_invocation_count,
                       emitted_by_invocation_id, emitted_by_runner_context_id
                FROM {self.tables.EVENTS}
                WHERE {where}
                ORDER BY event_timestamp
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()
        markers = [
            EventMarker(
                event_id=row[0],
                event_code=row[1],
                timestamp=datetime.fromtimestamp(row[2], tz=UTC),
                matched=row[3] > 0,
                triggered=row[4] > 0,
                emitted_by_invocation_id=row[5],
                emitted_by_runner_context_id=row[6],
            )
            for row in rows
        ]
        return EventMarkerPage(
            markers=markers,
            total=total,
            truncated=offset + len(markers) < total,
        )

    def link_trigger_run_to_events(
        self,
        event_ids: list[str],
        invocation_id: str,
        *,
        trigger_run_id: str,
    ) -> None:
        if not event_ids:
            return
        linked_at = datetime.now(UTC).timestamp()
        with sqlite_conn(self.sqlite_db_path) as conn:
            for event_id in event_ids:
                cur = conn.execute(
                    f"""
                    INSERT OR IGNORE INTO {self.tables.EVENT_TRIGGERED_INVOCATIONS}
                        (event_id, invocation_id, trigger_run_id, linked_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (event_id, invocation_id, trigger_run_id, linked_at),
                )
                if cur.rowcount > 0:
                    conn.execute(
                        f"UPDATE {self.tables.EVENTS} "
                        f"SET triggered_invocation_count = triggered_invocation_count + 1 "
                        f"WHERE event_id = ?",
                        (event_id,),
                    )
            conn.commit()

    def get_invocations_triggered_by_event(self, event_id: str) -> list[str]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            rows = conn.execute(
                f"SELECT invocation_id FROM {self.tables.EVENT_TRIGGERED_INVOCATIONS} "
                f"WHERE event_id = ? ORDER BY linked_at",
                (event_id,),
            ).fetchall()
        return [row[0] for row in rows]

    def _load_invocations_for_events(
        self, conn: "SQLiteConnection", event_ids: list[str]
    ) -> dict[str, list[str]]:
        """Bulk-load triggered invocation ids for a batch of event ids."""
        if not event_ids:
            return {}
        out: dict[str, list[str]] = {eid: [] for eid in event_ids}
        batch = 500
        for i in range(0, len(event_ids), batch):
            chunk = event_ids[i : i + batch]
            placeholders = ",".join("?" * len(chunk))
            rows = conn.execute(
                f"SELECT event_id, invocation_id "
                f"FROM {self.tables.EVENT_TRIGGERED_INVOCATIONS} "
                f"WHERE event_id IN ({placeholders}) ORDER BY linked_at",
                tuple(chunk),
            ).fetchall()
            for event_id, invocation_id in rows:
                out.setdefault(event_id, []).append(invocation_id)
        return out

    def list_event_codes(self) -> list[str]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            rows = conn.execute(
                f"SELECT DISTINCT event_code FROM {self.tables.EVENTS} "
                f"ORDER BY event_code"
            ).fetchall()
        return [row[0] for row in rows]

    def _build_event_where(
        self,
        event_code: str | None,
        start_time: datetime | None,
        end_time: datetime | None,
        matched: bool | None,
        triggered: bool | None,
        emitted_by_invocation_id: str | None = None,
        emitted_by_task_id: str | None = None,
    ) -> tuple[str, tuple]:
        """Build a parameterized WHERE clause for event filters."""
        clauses: list[str] = []
        params: list = []
        if event_code is not None:
            clauses.append("event_code = ?")
            params.append(event_code)
        if start_time is not None:
            clauses.append("event_timestamp >= ?")
            params.append(start_time.timestamp())
        if end_time is not None:
            clauses.append("event_timestamp <= ?")
            params.append(end_time.timestamp())
        if matched is True:
            clauses.append("matched_condition_count > 0")
        elif matched is False:
            clauses.append("matched_condition_count = 0")
        if triggered is True:
            clauses.append("triggered_invocation_count > 0")
        elif triggered is False:
            clauses.append("triggered_invocation_count = 0")
        if emitted_by_invocation_id is not None:
            clauses.append("emitted_by_invocation_id = ?")
            params.append(emitted_by_invocation_id)
        if emitted_by_task_id is not None:
            clauses.append("emitted_by_task_id = ?")
            params.append(emitted_by_task_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return where, tuple(params)

    # Trigger-run records and participant indexes. Participant rows keep the
    # event/source-invocation/valid-condition relations queryable without
    # deserializing every trigger-run JSON blob.

    def store_trigger_run(self, run: TriggerRunRecord) -> None:
        """Upsert a trigger run plus its per-condition link rows."""
        claimed_ts = run.claimed_at.timestamp() if run.claimed_at else None
        executed_ts = run.executed_at.timestamp() if run.executed_at else None
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                INSERT OR REPLACE INTO {self.tables.TRIGGER_RUNS} (
                    trigger_run_id, trigger_id, task_id_key, logic_value,
                    claimed_at, executed_at, triggered_invocation_id,
                    trigger_run_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.trigger_run_id,
                    run.trigger_id,
                    run.task_id_key,
                    run.logic_value,
                    claimed_ts,
                    executed_ts,
                    run.triggered_invocation_id,
                    run.to_json(),
                ),
            )
            conn.execute(
                f"DELETE FROM {self.tables.TRIGGER_RUN_CONDITIONS} "
                f"WHERE trigger_run_id = ?",
                (run.trigger_run_id,),
            )
            self._insert_trigger_run_conditions(conn, run)
            conn.commit()

    def _insert_trigger_run_conditions(
        self, conn: "SQLiteConnection", run: TriggerRunRecord
    ) -> None:
        """Write one row per participant (or per id when participants are absent)."""
        sql = (
            f"INSERT INTO {self.tables.TRIGGER_RUN_CONDITIONS} ("
            "trigger_run_id, context_type, condition_id, "
            "valid_condition_id, event_id, source_invocation_id"
            ") VALUES (?, ?, ?, ?, ?, ?)"
        )
        if run.participants:
            for p in run.participants:
                conn.execute(
                    sql,
                    (
                        run.trigger_run_id,
                        p.context_type,
                        p.condition_id,
                        p.valid_condition_id,
                        p.event_id,
                        p.source_invocation_id,
                    ),
                )
            return
        for event_id in run.event_ids:
            conn.execute(sql, (run.trigger_run_id, None, None, None, event_id, None))
        for invocation_id in run.source_invocation_ids:
            conn.execute(
                sql, (run.trigger_run_id, None, None, None, None, invocation_id)
            )

    def get_trigger_run(self, trigger_run_id: str) -> TriggerRunRecord | None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            row = conn.execute(
                f"SELECT trigger_run_json FROM {self.tables.TRIGGER_RUNS} "
                f"WHERE trigger_run_id = ?",
                (trigger_run_id,),
            ).fetchone()
        if not row:
            return None
        return TriggerRunRecord.from_json(row[0])

    def get_trigger_runs_for_event(self, event_id: str) -> list[TriggerRunRecord]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT t.trigger_run_json
                FROM {self.tables.TRIGGER_RUNS} t
                JOIN {self.tables.TRIGGER_RUN_CONDITIONS} c
                    ON c.trigger_run_id = t.trigger_run_id
                WHERE c.event_id = ?
                ORDER BY t.executed_at
                """,
                (event_id,),
            ).fetchall()
        return [TriggerRunRecord.from_json(row[0]) for row in rows]

    def get_trigger_runs_for_invocation(
        self, invocation_id: str
    ) -> list[TriggerRunRecord]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            rows = conn.execute(
                f"SELECT trigger_run_json FROM {self.tables.TRIGGER_RUNS} "
                f"WHERE triggered_invocation_id = ?",
                (invocation_id,),
            ).fetchall()
        return [TriggerRunRecord.from_json(row[0]) for row in rows]

    def get_trigger_runs_sourced_by_invocation(
        self, invocation_id: str
    ) -> list[TriggerRunRecord]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT t.trigger_run_json
                FROM {self.tables.TRIGGER_RUNS} t
                JOIN {self.tables.TRIGGER_RUN_CONDITIONS} c
                    ON c.trigger_run_id = t.trigger_run_id
                WHERE c.source_invocation_id = ?
                ORDER BY t.executed_at
                """,
                (invocation_id,),
            ).fetchall()
        return [TriggerRunRecord.from_json(row[0]) for row in rows]

    def get_trigger_runs_for_valid_condition(
        self, valid_condition_id: str
    ) -> list[TriggerRunRecord]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT t.trigger_run_json
                FROM {self.tables.TRIGGER_RUNS} t
                JOIN {self.tables.TRIGGER_RUN_CONDITIONS} c
                    ON c.trigger_run_id = t.trigger_run_id
                WHERE c.valid_condition_id = ?
                ORDER BY t.executed_at
                """,
                (valid_condition_id,),
            ).fetchall()
        return [TriggerRunRecord.from_json(row[0]) for row in rows]

    def get_trigger_runs_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        *,
        event_code: str | None = None,
        task_id_key: str | None = None,
        limit: int | None = None,
    ) -> list[TriggerRunRecord]:
        clauses = ["t.executed_at IS NOT NULL"]
        params: list = []
        clauses.append("t.executed_at >= ? AND t.executed_at <= ?")
        params.extend([start_time.timestamp(), end_time.timestamp()])
        join = ""
        if task_id_key is not None:
            clauses.append("t.task_id_key = ?")
            params.append(task_id_key)
        if event_code is not None:
            join = (
                f" JOIN {self.tables.TRIGGER_RUN_CONDITIONS} c "
                f"ON c.trigger_run_id = t.trigger_run_id "
                f"JOIN {self.tables.EVENTS} e ON e.event_id = c.event_id"
            )
            clauses.append("e.event_code = ?")
            params.append(event_code)
        sql = (
            f"SELECT DISTINCT t.trigger_run_json FROM {self.tables.TRIGGER_RUNS} t"
            f"{join} WHERE {' AND '.join(clauses)} ORDER BY t.executed_at"
        )
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with sqlite_conn(self.sqlite_db_path) as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [TriggerRunRecord.from_json(row[0]) for row in rows]

    # ------------------------------------------------------------------ #
    # Monitoring retention
    # ------------------------------------------------------------------ #
    # Retention applies age and capacity limits to events and trigger runs,
    # cascading event deletes into trigger-run tables to avoid stale edges.
    # The driving algorithm lives in BaseTrigger._auto_purge_events; this
    # class supplies the SQLite primitives.

    def _age_purge_events(self, threshold: datetime) -> list[str]:
        ts = threshold.timestamp()
        with sqlite_conn(self.sqlite_db_path) as conn:
            removed = self._purge_events_age(conn, ts)
            conn.commit()
        return removed

    def _age_purge_trigger_runs(self, threshold: datetime) -> int:
        ts = threshold.timestamp()
        with sqlite_conn(self.sqlite_db_path) as conn:
            count = self._purge_trigger_runs_age(conn, ts)
            conn.commit()
        return count

    def _cap_purge_events(self) -> list[str]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            removed = self._enforce_event_capacity(conn)
            conn.commit()
        return removed

    def _cap_purge_trigger_runs(self) -> int:
        with sqlite_conn(self.sqlite_db_path) as conn:
            count = self._enforce_trigger_run_capacity(conn)
            conn.commit()
        return count

    def _cascade_delete_runs_for_events(self, event_ids: list[str]) -> int:
        with sqlite_conn(self.sqlite_db_path) as conn:
            count = self._cascade_delete_runs_for_events_in_conn(conn, event_ids)
            conn.commit()
        return count

    def _cascade_delete_runs_for_events_in_conn(
        self, conn: "SQLiteConnection", event_ids: list[str]
    ) -> int:
        """Delete trigger runs that reference any of the given event ids."""
        unique_ids = list(dict.fromkeys(event_ids))
        run_ids: list[str] = []
        batch = 500
        for i in range(0, len(unique_ids), batch):
            chunk = unique_ids[i : i + batch]
            placeholders = ",".join("?" * len(chunk))
            rows = conn.execute(
                f"SELECT DISTINCT trigger_run_id "
                f"FROM {self.tables.TRIGGER_RUN_CONDITIONS} "
                f"WHERE event_id IN ({placeholders})",
                tuple(chunk),
            ).fetchall()
            run_ids.extend(row[0] for row in rows)
        if not run_ids:
            return 0
        _delete_by_ids(
            conn, self.tables.TRIGGER_RUN_CONDITIONS, "trigger_run_id", run_ids
        )
        _delete_by_ids(conn, self.tables.TRIGGER_RUNS, "trigger_run_id", run_ids)
        return len(run_ids)

    def _purge_events_age(
        self, conn: "SQLiteConnection", threshold: float
    ) -> list[str]:
        """Delete events older than ``threshold``; return removed event ids."""
        ids = [
            row[0]
            for row in conn.execute(
                f"SELECT event_id FROM {self.tables.EVENTS} WHERE event_timestamp < ?",
                (threshold,),
            ).fetchall()
        ]
        if not ids:
            return []
        _delete_by_ids(conn, self.tables.EVENT_CONDITIONS, "event_id", ids)
        _delete_by_ids(conn, self.tables.EVENT_TRIGGERED_INVOCATIONS, "event_id", ids)
        _delete_by_ids(conn, self.tables.EVENTS, "event_id", ids)
        return ids

    def _purge_trigger_runs_age(
        self, conn: "SQLiteConnection", threshold: float
    ) -> int:
        """Delete trigger runs whose ``executed_at`` is older than ``threshold``."""
        ids = [
            row[0]
            for row in conn.execute(
                f"SELECT trigger_run_id FROM {self.tables.TRIGGER_RUNS} "
                f"WHERE executed_at IS NOT NULL AND executed_at < ?",
                (threshold,),
            ).fetchall()
        ]
        if not ids:
            return 0
        _delete_by_ids(conn, self.tables.TRIGGER_RUN_CONDITIONS, "trigger_run_id", ids)
        _delete_by_ids(conn, self.tables.TRIGGER_RUNS, "trigger_run_id", ids)
        return len(ids)

    def _enforce_event_capacity(self, conn: "SQLiteConnection") -> list[str]:
        """Drop oldest events beyond ``event_max_records``; return removed ids."""
        cap = self.conf.event_max_records
        if cap <= 0:
            return []
        ids = [
            row[0]
            for row in conn.execute(
                f"SELECT event_id FROM {self.tables.EVENTS} "
                f"ORDER BY event_timestamp DESC LIMIT -1 OFFSET ?",
                (cap,),
            ).fetchall()
        ]
        if not ids:
            return []
        _delete_by_ids(conn, self.tables.EVENT_CONDITIONS, "event_id", ids)
        _delete_by_ids(conn, self.tables.EVENT_TRIGGERED_INVOCATIONS, "event_id", ids)
        _delete_by_ids(conn, self.tables.EVENTS, "event_id", ids)
        return ids

    def _enforce_trigger_run_capacity(self, conn: "SQLiteConnection") -> int:
        """Drop oldest trigger runs beyond ``trigger_run_max_records``."""
        cap = self.conf.trigger_run_max_records
        if cap <= 0:
            return 0
        ids = [
            row[0]
            for row in conn.execute(
                f"SELECT trigger_run_id FROM {self.tables.TRIGGER_RUNS} "
                f"ORDER BY COALESCE(executed_at, claimed_at, 0) DESC "
                f"LIMIT -1 OFFSET ?",
                (cap,),
            ).fetchall()
        ]
        if not ids:
            return 0
        _delete_by_ids(conn, self.tables.TRIGGER_RUN_CONDITIONS, "trigger_run_id", ids)
        _delete_by_ids(conn, self.tables.TRIGGER_RUNS, "trigger_run_id", ids)
        return len(ids)


# ---------------------------------------------------------------------- #
# SQLite helper functions
# ---------------------------------------------------------------------- #
# Small stateless helpers stay outside SQLiteTrigger so table-specific methods
# can share batching behavior without adding inheritance layers.


def _delete_by_ids(
    conn: "SQLiteConnection", table: str, column: str, ids: list[str]
) -> None:
    """Delete rows from ``table`` where ``column`` is in ``ids`` in batches."""
    batch = 500
    for i in range(0, len(ids), batch):
        chunk = ids[i : i + batch]
        placeholders = ",".join("?" * len(chunk))
        conn.execute(
            f"DELETE FROM {table} WHERE {column} IN ({placeholders})", tuple(chunk)
        )

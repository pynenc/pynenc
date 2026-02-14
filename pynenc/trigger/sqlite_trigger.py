"""
SQLite-based implementation of the Pynenc trigger subsystem.

This module provides a trigger system implementation that stores
all its state in a SQLite database. Suitable for cross-process coordination and testing.
"""

from collections.abc import Iterable
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Optional

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
from pynenc.util.sqlite_utils import create_sqlite_connection as sqlite_conn
from pynenc.util.sqlite_utils import delete_tables_with_prefix

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class Tables:
    CONDITIONS = "trg_conditions"
    TRIGGERS = "trg_triggers"
    CONDITION_TRIGGERS = "trg_condition_triggers"
    VALID_CONDITIONS = "trg_valid_conditions"
    SOURCE_TASK_CONDITIONS = "trg_source_task_conditions"
    EXECUTION_CLAIMS = "trg_execution_claims"
    TRIGGER_RUN_CLAIMS = "trg_trigger_run_claims"


class SQLiteTrigger(BaseTrigger):
    """
    SQLite-based implementation of the Pynenc trigger system.

    Stores all trigger, condition, and claim data in a SQLite database for cross-process safety.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.sqlite_db_path = self.conf.sqlite_db_path
        self._init_tables()

    @cached_property
    def conf(self) -> ConfigTriggerSQLite:
        return ConfigTriggerSQLite(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def _init_tables(self) -> None:
        """Initialize SQLite tables for trigger state."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.CONDITIONS} (
                    condition_id TEXT PRIMARY KEY,
                    condition_json TEXT NOT NULL,
                    last_cron_execution TIMESTAMP
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.TRIGGERS} (
                    trigger_id TEXT PRIMARY KEY,
                    task_id_key TEXT NOT NULL,
                    logic_value TEXT NOT NULL,
                    argument_provider_json TEXT
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.CONDITION_TRIGGERS} (
                    condition_id TEXT NOT NULL,
                    trigger_id TEXT NOT NULL,
                    PRIMARY KEY (condition_id, trigger_id)
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.VALID_CONDITIONS} (
                    valid_condition_id TEXT PRIMARY KEY,
                    valid_condition_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.SOURCE_TASK_CONDITIONS} (
                    task_id_key TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    PRIMARY KEY (task_id_key, condition_id)
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.EXECUTION_CLAIMS} (
                    claim_key TEXT PRIMARY KEY,
                    expiration TIMESTAMP
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.TRIGGER_RUN_CLAIMS} (
                    trigger_run_id TEXT PRIMARY KEY,
                    expiration TIMESTAMP
                )
                """
            )
            conn.commit()

    def _register_condition(self, condition: TriggerCondition) -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.CONDITIONS} (condition_id, condition_json) VALUES (?, ?)",
                (condition.condition_id, condition.to_json(self.app)),
            )
            conn.commit()

    def get_condition(self, condition_id: str) -> TriggerCondition | None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT condition_json FROM {Tables.CONDITIONS} WHERE condition_id = ?",
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
                f"INSERT OR REPLACE INTO {Tables.TRIGGERS} (trigger_id, task_id_key, logic_value, argument_provider_json) VALUES (?, ?, ?, ?)",
                (
                    trigger.trigger_id,
                    trigger.task_id.key,
                    trigger.logic.value,
                    trigger.argument_provider_json,
                ),
            )
            for condition_id in trigger.condition_ids:
                conn.execute(
                    f"INSERT OR REPLACE INTO {Tables.CONDITION_TRIGGERS} (condition_id, trigger_id) VALUES (?, ?)",
                    (condition_id, trigger.trigger_id),
                )
            conn.commit()

    def _get_trigger(self, trigger_id: str) -> Optional["TriggerDefinitionDTO"]:
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
                f"SELECT trigger_id, task_id_key, logic_value, argument_provider_json FROM {Tables.TRIGGERS} WHERE trigger_id IN ({trg_ids_query})",
                tuple(trigger_ids),
            )
            triggers_rows = cursor.fetchall()
            cursor.close()
            if not triggers_rows:
                return []

            # Fetch condition ids for all triggers in one query
            cursor = conn.execute(
                f"SELECT condition_id, trigger_id FROM {Tables.CONDITION_TRIGGERS} WHERE trigger_id IN ({trg_ids_query})",
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
                f"SELECT trigger_id FROM {Tables.CONDITION_TRIGGERS} WHERE condition_id = ?",
                (condition_id,),
            )
            trigger_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            # Fetch all triggers in batch
            return self._get_triggers(trigger_ids)

    def record_valid_condition(self, valid_condition: ValidCondition) -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.VALID_CONDITIONS} (valid_condition_id, valid_condition_json) VALUES (?, ?)",
                (valid_condition.valid_condition_id, valid_condition.to_json(self.app)),
            )
            conn.commit()

    def record_valid_conditions(self, valid_conditions: list[ValidCondition]) -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            for valid_condition in valid_conditions:
                conn.execute(
                    f"INSERT OR REPLACE INTO {Tables.VALID_CONDITIONS} (valid_condition_id, valid_condition_json) VALUES (?, ?)",
                    (
                        valid_condition.valid_condition_id,
                        valid_condition.to_json(self.app),
                    ),
                )
            conn.commit()

    def get_valid_conditions(self) -> dict[str, ValidCondition]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT valid_condition_id, valid_condition_json FROM {Tables.VALID_CONDITIONS}"
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
                    f"DELETE FROM {Tables.VALID_CONDITIONS} WHERE valid_condition_id = ?",
                    (condition.valid_condition_id,),
                )
            conn.commit()

    def _get_all_conditions(self) -> list[TriggerCondition]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(f"SELECT condition_json FROM {Tables.CONDITIONS}")
            cursor_rows = cursor.fetchall()
            cursor.close()
            return [TriggerCondition.from_json(row[0], self.app) for row in cursor_rows]

    def get_last_cron_execution(self, condition_id: str) -> datetime | None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT last_cron_execution FROM {Tables.CONDITIONS} WHERE condition_id = ?",
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
                f"SELECT last_cron_execution FROM {Tables.CONDITIONS} WHERE condition_id = ?",
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
                f"UPDATE {Tables.CONDITIONS} SET last_cron_execution = ? WHERE condition_id = ?",
                (execution_time.isoformat(), condition_id),
            )
            conn.commit()
            return True

    def _register_source_task_condition(
        self, task_id: "TaskId", condition_id: str
    ) -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.SOURCE_TASK_CONDITIONS} (task_id_key, condition_id) VALUES (?, ?)",
                (task_id.key, condition_id),
            )
            conn.commit()

    def get_conditions_sourced_from_task(
        self, task_id: "TaskId", context_type: type[ConditionContext] | None = None
    ) -> list[TriggerCondition]:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT condition_id FROM {Tables.SOURCE_TASK_CONDITIONS} WHERE task_id_key = ?",
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
                f"SELECT expiration FROM {Tables.EXECUTION_CLAIMS} WHERE claim_key = ?",
                (claim_key,),
            )
            row = cursor.fetchone()
            cursor.close()
            if row and row[0]:
                existing_expiration = datetime.fromisoformat(row[0])
                if existing_expiration > now:
                    return False
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.EXECUTION_CLAIMS} (claim_key, expiration) VALUES (?, ?)",
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
                f"SELECT expiration FROM {Tables.TRIGGER_RUN_CLAIMS} WHERE trigger_run_id = ?",
                (trigger_run_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if row and row[0]:
                existing_expiration = datetime.fromisoformat(row[0])
                if existing_expiration > now:
                    return False
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.TRIGGER_RUN_CLAIMS} (trigger_run_id, expiration) VALUES (?, ?)",
                (trigger_run_id, expiration.isoformat()),
            )
            conn.commit()
            return True

    def clean_task_trigger_definitions(self, task_id: "TaskId") -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT trigger_id FROM {Tables.TRIGGERS} WHERE task_id_key = ?",
                (task_id.key,),
            )
            trigger_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            for trigger_id in trigger_ids:
                conn.execute(
                    f"DELETE FROM {Tables.TRIGGERS} WHERE trigger_id = ?", (trigger_id,)
                )
                conn.execute(
                    f"DELETE FROM {Tables.CONDITION_TRIGGERS} WHERE trigger_id = ?",
                    (trigger_id,),
                )
            conn.commit()

    def _purge(self) -> None:
        delete_tables_with_prefix(self.sqlite_db_path, "trg_")
        self._init_tables()

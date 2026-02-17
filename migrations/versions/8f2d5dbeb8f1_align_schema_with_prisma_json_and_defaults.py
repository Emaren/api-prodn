"""Align schema with Prisma JSON/default semantics.

Revision ID: 8f2d5dbeb8f1
Revises: 173e2e09e57f
Create Date: 2026-02-17 06:00:00.000000
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "8f2d5dbeb8f1"
down_revision = "173e2e09e57f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Helper to safely parse text into jsonb without aborting migration on bad rows.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public._try_parse_jsonb(input text)
        RETURNS jsonb
        LANGUAGE plpgsql
        AS $$
        BEGIN
          RETURN input::jsonb;
        EXCEPTION WHEN others THEN
          RETURN NULL;
        END;
        $$;
        """
    )

    # --- users table alignment ---
    op.execute(
        """
        ALTER TABLE public.users
          ADD COLUMN IF NOT EXISTS last_seen timestamp without time zone,
          ADD COLUMN IF NOT EXISTS is_admin boolean;
        """
    )

    op.execute(
        """
        ALTER TABLE public.users
          ALTER COLUMN uid TYPE varchar(100),
          ALTER COLUMN email TYPE varchar(100),
          ALTER COLUMN token TYPE varchar(128);
        """
    )

    op.execute("UPDATE public.users SET verified = FALSE WHERE verified IS NULL;")
    op.execute("UPDATE public.users SET lock_name = FALSE WHERE lock_name IS NULL;")
    op.execute("UPDATE public.users SET is_admin = FALSE WHERE is_admin IS NULL;")
    op.execute("UPDATE public.users SET created_at = NOW() WHERE created_at IS NULL;")

    op.execute("ALTER TABLE public.users ALTER COLUMN verified SET DEFAULT FALSE;")
    op.execute("ALTER TABLE public.users ALTER COLUMN verified SET NOT NULL;")
    op.execute("ALTER TABLE public.users ALTER COLUMN lock_name SET DEFAULT FALSE;")
    op.execute("ALTER TABLE public.users ALTER COLUMN lock_name SET NOT NULL;")
    op.execute("ALTER TABLE public.users ALTER COLUMN is_admin SET DEFAULT FALSE;")
    op.execute("ALTER TABLE public.users ALTER COLUMN is_admin SET NOT NULL;")
    op.execute("ALTER TABLE public.users ALTER COLUMN created_at SET DEFAULT NOW();")
    op.execute("ALTER TABLE public.users ALTER COLUMN created_at SET NOT NULL;")

    # --- game_stats table alignment ---
    op.execute(
        """
        ALTER TABLE public.game_stats
          ALTER COLUMN user_uid TYPE varchar(100),
          ALTER COLUMN replay_file TYPE varchar(500),
          ALTER COLUMN replay_hash TYPE varchar(64),
          ALTER COLUMN game_version TYPE varchar(50),
          ALTER COLUMN game_type TYPE varchar(50),
          ALTER COLUMN winner TYPE varchar(100),
          ALTER COLUMN parse_source TYPE varchar(20),
          ALTER COLUMN parse_reason TYPE varchar(50),
          ALTER COLUMN original_filename TYPE varchar(255);
        """
    )

    # map: varchar/json -> jsonb
    op.execute(
        """
        DO $$
        DECLARE col_type text;
        BEGIN
          SELECT data_type
            INTO col_type
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = 'game_stats'
            AND column_name = 'map';

          IF col_type IN ('character varying', 'text') THEN
            ALTER TABLE public.game_stats
              ALTER COLUMN map TYPE jsonb
              USING CASE
                WHEN map IS NULL OR btrim(map::text) = '' THEN NULL
                ELSE COALESCE(
                  public._try_parse_jsonb(map::text),
                  jsonb_build_object('name', map::text, 'size', 'Unknown')
                )
              END;
          ELSIF col_type = 'json' THEN
            ALTER TABLE public.game_stats
              ALTER COLUMN map TYPE jsonb
              USING map::jsonb;
          END IF;
        END $$;
        """
    )

    # players/event_types/key_events -> jsonb
    op.execute(
        """
        DO $$
        DECLARE col_type text;
        BEGIN
          SELECT data_type INTO col_type
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = 'game_stats'
            AND column_name = 'players';

          IF col_type = 'json' THEN
            ALTER TABLE public.game_stats
              ALTER COLUMN players TYPE jsonb
              USING players::jsonb;
          ELSIF col_type IN ('character varying', 'text') THEN
            ALTER TABLE public.game_stats
              ALTER COLUMN players TYPE jsonb
              USING COALESCE(
                public._try_parse_jsonb(players::text),
                jsonb_build_array(players::text)
              );
          END IF;
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        DECLARE col_type text;
        BEGIN
          SELECT data_type INTO col_type
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = 'game_stats'
            AND column_name = 'event_types';

          IF col_type = 'json' THEN
            ALTER TABLE public.game_stats
              ALTER COLUMN event_types TYPE jsonb
              USING event_types::jsonb;
          ELSIF col_type IN ('character varying', 'text') THEN
            ALTER TABLE public.game_stats
              ALTER COLUMN event_types TYPE jsonb
              USING COALESCE(
                public._try_parse_jsonb(event_types::text),
                '[]'::jsonb
              );
          END IF;
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        DECLARE col_type text;
        BEGIN
          SELECT data_type INTO col_type
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = 'game_stats'
            AND column_name = 'key_events';

          IF col_type = 'json' THEN
            ALTER TABLE public.game_stats
              ALTER COLUMN key_events TYPE jsonb
              USING key_events::jsonb;
          ELSIF col_type IN ('character varying', 'text') THEN
            ALTER TABLE public.game_stats
              ALTER COLUMN key_events TYPE jsonb
              USING COALESCE(
                public._try_parse_jsonb(key_events::text),
                '{}'::jsonb
              );
          END IF;
        END $$;
        """
    )

    # Normalize legacy json-string rows.
    op.execute(
        """
        UPDATE public.game_stats
        SET map = CASE
          WHEN map IS NULL THEN NULL
          WHEN jsonb_typeof(map) = 'string' THEN COALESCE(
            public._try_parse_jsonb(map #>> '{}'),
            jsonb_build_object('name', map #>> '{}', 'size', 'Unknown')
          )
          ELSE map
        END;
        """
    )

    op.execute(
        """
        UPDATE public.game_stats
        SET players = CASE
          WHEN players IS NULL THEN NULL
          WHEN jsonb_typeof(players) = 'string' THEN COALESCE(
            public._try_parse_jsonb(players #>> '{}'),
            jsonb_build_array(players #>> '{}')
          )
          ELSE players
        END;
        """
    )

    op.execute(
        """
        UPDATE public.game_stats
        SET event_types = CASE
          WHEN event_types IS NULL THEN NULL
          WHEN jsonb_typeof(event_types) = 'string' THEN COALESCE(
            public._try_parse_jsonb(event_types #>> '{}'),
            '[]'::jsonb
          )
          ELSE event_types
        END;
        """
    )

    op.execute(
        """
        UPDATE public.game_stats
        SET key_events = CASE
          WHEN key_events IS NULL THEN NULL
          WHEN jsonb_typeof(key_events) = 'string' THEN COALESCE(
            public._try_parse_jsonb(key_events #>> '{}'),
            '{}'::jsonb
          )
          ELSE key_events
        END;
        """
    )

    op.execute("UPDATE public.game_stats SET parse_iteration = 0 WHERE parse_iteration IS NULL;")
    op.execute("UPDATE public.game_stats SET is_final = FALSE WHERE is_final IS NULL;")
    op.execute(
        "UPDATE public.game_stats SET disconnect_detected = FALSE WHERE disconnect_detected IS NULL;"
    )
    op.execute(
        """
        UPDATE public.game_stats
        SET parse_source = 'unknown'
        WHERE parse_source IS NULL OR btrim(parse_source) = '';
        """
    )
    op.execute(
        """
        UPDATE public.game_stats
        SET parse_reason = 'unspecified'
        WHERE parse_reason IS NULL OR btrim(parse_reason) = '';
        """
    )
    op.execute("UPDATE public.game_stats SET created_at = NOW() WHERE created_at IS NULL;")

    op.execute("ALTER TABLE public.game_stats ALTER COLUMN parse_iteration SET DEFAULT 0;")
    op.execute("ALTER TABLE public.game_stats ALTER COLUMN parse_iteration SET NOT NULL;")
    op.execute("ALTER TABLE public.game_stats ALTER COLUMN is_final SET DEFAULT FALSE;")
    op.execute("ALTER TABLE public.game_stats ALTER COLUMN is_final SET NOT NULL;")
    op.execute("ALTER TABLE public.game_stats ALTER COLUMN disconnect_detected SET DEFAULT FALSE;")
    op.execute("ALTER TABLE public.game_stats ALTER COLUMN disconnect_detected SET NOT NULL;")
    op.execute("ALTER TABLE public.game_stats ALTER COLUMN parse_source SET DEFAULT 'unknown';")
    op.execute("ALTER TABLE public.game_stats ALTER COLUMN parse_source SET NOT NULL;")
    op.execute("ALTER TABLE public.game_stats ALTER COLUMN parse_reason SET DEFAULT 'unspecified';")
    op.execute("ALTER TABLE public.game_stats ALTER COLUMN parse_reason SET NOT NULL;")
    op.execute("ALTER TABLE public.game_stats ALTER COLUMN created_at SET DEFAULT NOW();")
    op.execute("ALTER TABLE public.game_stats ALTER COLUMN created_at SET NOT NULL;")
    op.execute('ALTER TABLE public.game_stats ALTER COLUMN "timestamp" SET DEFAULT NOW();')

    # Keep important indexes present for query performance/uniqueness.
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_game_stats_user_uid
        ON public.game_stats USING btree (user_uid);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_replay_iteration
        ON public.game_stats USING btree (replay_file, parse_iteration);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_replay_hash_iteration
        ON public.game_stats USING btree (replay_hash, parse_iteration);
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'uq_replay_final'
              AND conrelid = 'public.game_stats'::regclass
          ) THEN
            ALTER TABLE public.game_stats
              ADD CONSTRAINT uq_replay_final UNIQUE (replay_hash, is_final);
          END IF;
        END $$;
        """
    )

    op.execute("DROP FUNCTION IF EXISTS public._try_parse_jsonb(text);")


def downgrade() -> None:
    # Intentionally left as a no-op: this migration normalizes live data types and defaults.
    pass

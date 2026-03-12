"""Add replay parse attempts table.

Revision ID: b7c5c7c4f2de
Revises: 8f2d5dbeb8f1
Create Date: 2026-03-12 10:10:00.000000
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "b7c5c7c4f2de"
down_revision = "8f2d5dbeb8f1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.replay_parse_attempts (
          id SERIAL PRIMARY KEY,
          created_at timestamp without time zone NOT NULL DEFAULT NOW(),
          user_uid varchar(100),
          replay_hash varchar(64),
          original_filename varchar(255),
          parse_source varchar(20) NOT NULL DEFAULT 'file_upload',
          status varchar(32) NOT NULL DEFAULT 'received',
          detail varchar(255),
          upload_mode varchar(20),
          file_size_bytes integer,
          game_stats_id integer,
          played_on timestamp without time zone
        );
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'replay_parse_attempts_game_stats_id_fkey'
          ) THEN
            ALTER TABLE public.replay_parse_attempts
              ADD CONSTRAINT replay_parse_attempts_game_stats_id_fkey
              FOREIGN KEY (game_stats_id)
              REFERENCES public.game_stats(id)
              ON DELETE SET NULL
              ON UPDATE NO ACTION;
          END IF;
        END $$;
        """
    )

    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_replay_parse_attempts_created_at ON public.replay_parse_attempts (created_at);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_replay_parse_attempts_status_created_at ON public.replay_parse_attempts (status, created_at);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_replay_parse_attempts_user_uid_created_at ON public.replay_parse_attempts (user_uid, created_at);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_replay_parse_attempts_replay_hash ON public.replay_parse_attempts (replay_hash);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_replay_parse_attempts_game_stats_id ON public.replay_parse_attempts (game_stats_id);"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS public.replay_parse_attempts;")

from utils import get_connection
def create_batch(cur, batch_id, total_symbols):
    cur.execute(
        """
        INSERT INTO etl_batch (
            batch_id,
            started_at,
            status,
            total_symbols
        )
        VALUES (
            %s,
            NOW(),
            'RUNNING',
            %s
        )
        """,
        (batch_id, total_symbols )
    )
    
def create_symbol_record(cur, batch_id, symbol):
    cur.execute(
        """
        INSERT INTO etl_batch_symbol (
            batch_id,
            symbol,
            started_at,
            status
        )
        VALUES (
            %s,
            %s,
            NOW(),
            'RUNNING'
        )
        """,
        (batch_id, symbol)
    )
    
def mark_symbol_success(cur, batch_id, symbol, rows_loaded):
    cur.execute(
        """
        UPDATE etl_batch_symbol
        SET finished_at=NOW(), status='SUCCESS', rows_loaded=%s
        WHERE batch_id=%s and symbol=%s
        """,
        (rows_loaded,batch_id,symbol)
    )

def mark_symbol_failed(cur, batch_id, symbol, error_message):
    cur.execute(
        """
        UPDATE etl_batch_symbol
        SET finished_at=NOW(), status='FAILED', error_message=%s
        WHERE batch_id=%s and symbol=%s
        """,
        (error_message,batch_id,symbol)
    )
    
def mark_batch_status(cur, batch_id: str) -> None:
    cur.execute(
        """
        UPDATE etl_batch_symbol
        SET status = 'FAILED'
        WHERE batch_id = %s
        AND status = 'RUNNING'
        """,
        (batch_id,)
    )

    cur.execute(
        """
        UPDATE etl_batch
        SET
            finished_at = NOW(),
            status = (
                SELECT
                    CASE
                        WHEN COUNT(*) = COUNT(*) FILTER (WHERE status = 'SUCCESS')
                            THEN 'SUCCESS'

                        WHEN COUNT(*) = COUNT(*) FILTER (WHERE status = 'FAILED')
                            THEN 'FAILED'

                        ELSE 'PARTIAL_SUCCESS'
                    END
                FROM etl_batch_symbol
                WHERE batch_id = %s
            ),
            total_rows_loaded = (
                SELECT COALESCE(SUM(rows_loaded), 0)
                FROM etl_batch_symbol
                WHERE batch_id = %s
            )
        WHERE batch_id = %s
        """,
        (batch_id, batch_id, batch_id)
    )
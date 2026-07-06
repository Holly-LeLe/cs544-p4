import grpc
from concurrent import futures
import time
import os
import subprocess
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from sqlalchemy import create_engine


import lender_pb2
import lender_pb2_grpc

class LenderServicer(lender_pb2_grpc.LenderServicer):
    """
    Implements the Lender service as defined in lender.proto.
    """

    def DbToHdfs(self, request, context):
        """
        Query Conventional loans from MySQL and write them to HDFS as Parquet.
        """
        print("Received DbToHdfs request")

        try:
            engine = create_engine("mysql+pymysql://root:acid@mysql:3306/CS544")

            query = """
                SELECT loans.*
                FROM loans
                INNER JOIN loan_types
                    ON loans.loan_type_id = loan_types.id
                WHERE loan_types.loan_type_name = 'Conventional'
            """

            # Retry because MySQL may still be starting when the RPC is called.
            last_err = None
            df = None
            for _ in range(30):
                try:
                    with engine.connect() as conn:
                        df = pd.read_sql(query, conn)
                    break
                except Exception as e:
                    last_err = e
                    time.sleep(2)

            if df is None:
                return lender_pb2.DbToHdfsResp(row_count=0, error=str(last_err))

            os.environ["CLASSPATH"] = subprocess.check_output(
                ["hadoop", "classpath", "--glob"], text=True
            ).strip()

            hdfs = pafs.HadoopFileSystem(
                host="nn",
                port=9000,
                replication=2,
                default_block_size=1024 * 1024,
            )

            table = pa.Table.from_pandas(df, preserve_index=False)
            with hdfs.open_output_stream("/hdma-wi-2021.parquet") as out:
                pq.write_table(table, out)

            return lender_pb2.DbToHdfsResp(row_count=len(df), error="")
        except Exception as e:
            return lender_pb2.DbToHdfsResp(row_count=0, error=str(e))

    def BlockLocations(self, request, context):
        """
        Get the block locations of the Parquet file in HDFS.
        """
        print(f"Received BlockLocations request for path: {request.path}")
        return lender_pb2.BlockLocationsResp(block_entries={}, error="not implemented")

    def CalcAvgLoan(self, request, context):
        """
        Calculate the average loan amount for a given county_code.
        """
        print(f"Received CalcAvgLoan request for county_code: {request.county_code}")
        return lender_pb2.CalcAvgLoanResp(avg_loan=0, source="not implemented", error="not implemented")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    lender_pb2_grpc.add_LenderServicer_to_server(LenderServicer(), server)
    server.add_insecure_port('[::]:5000')
    server.start()
    print("Server started, listening on port 5000")
    try:
        while True:
            time.sleep(86400) # One day in seconds
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()

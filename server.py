import grpc
from concurrent import futures
import time

import lender_pb2
import lender_pb2_grpc

class LenderServicer(lender_pb2_grpc.LenderServicer):
    """
    Implements the Lender service as defined in lender.proto.
    """

    def DbToHdfs(self, request, context):
        """
        Load input.data from SQL server and upload it to HDFS.
        """
        print("Received DbToHdfs request")
        return lender_pb2.DbToHdfsResp(row_count=0, error="not implemented")

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

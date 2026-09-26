"""Service-level test for the RelayMesh AI gRPC service.

Runs an in-process server on an ephemeral port and asserts the full
request/response contract the Go data plane depends on — most
importantly that every recommendation carries a usable path.
"""
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
# Generated grpc stubs import relaymesh_pb2 as a top-level module.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'service'))

import grpc
from concurrent import futures

from ai.service import relaymesh_pb2
from ai.service import relaymesh_pb2_grpc
from ai.service.server import AIServicer


def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    relaymesh_pb2_grpc.add_AIServiceServicer_to_server(AIServicer(), server)
    port = server.add_insecure_port('127.0.0.1:0')
    server.start()
    try:
        channel = grpc.insecure_channel(f'127.0.0.1:{port}')
        stub = relaymesh_pb2_grpc.AIServiceStub(channel)

        req = relaymesh_pb2.InferenceRequest(
            state=relaymesh_pb2.NetworkState(
                node_id='node-A',
                feature_vector=[10.0, 0.01, 100.0, 0, 0, 2, 1, 0, 0, 0],
            ),
            candidate_destinations=['node-B', 'node-C'],
        )
        resp = stub.GetRecommendation(req, timeout=5)
        assert len(resp.recommendations) == 1, 'expected one recommendation'
        rec = resp.recommendations[0]
        assert rec.destination in ('node-B', 'node-C'), rec.destination
        assert list(rec.path) == ['node-A', rec.destination], list(rec.path)
        assert rec.confidence > 0, rec.confidence
        assert resp.inference_time_ms >= 0
        print(f'GetRecommendation OK: dest={rec.destination} path={list(rec.path)}')

        fb = stub.ReportFeedback(relaymesh_pb2.TrainingFeedback(
            route_id='node-A-node-B', reward=8.5, success=True), timeout=5)
        assert fb.success, 'feedback ack failed'
        print('ReportFeedback OK')

        info = stub.GetModelInfo(relaymesh_pb2.ModelInfoRequest(node_id='x'), timeout=5)
        assert info.model_name == 'relaymesh-rl', info.model_name
        print(f'GetModelInfo OK: {info.model_name} v{info.model_version}')

        print('ALL AI SERVICE TESTS PASSED')
    finally:
        server.stop(0)


if __name__ == '__main__':
    main()

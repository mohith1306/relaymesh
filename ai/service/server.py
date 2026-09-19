"""gRPC server for AI service."""
import grpc
from concurrent import futures
import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

try:
    import relaymesh_pb2
    import relaymesh_pb2_grpc
except ImportError:
    print("Proto files not generated. Run: protoc --python_out=. --grpc_python_out=. relaymesh.proto")
    sys.exit(1)

from .inference import RouteInference


class AIServicer(relaymesh_pb2_grpc.AIServiceServicer):
    """AI service implementation."""
    
    def __init__(self):
        self.inference = RouteInference()
        
    def GetRecommendation(self, request, context):
        """Get route recommendation."""
        state = {
            'node_id': request.state.node_id,
            'metrics': {
                'latency': request.state.feature_vector[0] if len(request.state.feature_vector) > 0 else 0.0,
                'packet_loss': request.state.feature_vector[1] if len(request.state.feature_vector) > 1 else 0.0,
                'bandwidth': request.state.feature_vector[2] if len(request.state.feature_vector) > 2 else 0.0,
            }
        }
        
        candidates = list(request.candidate_destinations)
        
        start_time = time.time()
        result = self.inference.get_recommendation(state, candidates)
        inference_time = int((time.time() - start_time) * 1000)
        
        recommendation = relaymesh_pb2.RouteRecommendation(
            source=request.state.node_id,
            destination=result.get('destination', ''),
            confidence=result.get('confidence', 0.0),
            score=result.get('score', 0.0),
            timestamp=int(time.time()),
        )
        
        return relaymesh_pb2.InferenceResponse(
            recommendations=[recommendation],
            inference_time_ms=inference_time,
        )
    
    def ReportFeedback(self, request, context):
        """Receive training feedback."""
        self.inference.update_from_feedback(request.route_id, request.reward)
        return relaymesh_pb2.Ack(
            success=True,
            message="Feedback received",
            timestamp=int(time.time()),
        )
    
    def GetModelInfo(self, request, context):
        """Get model information."""
        return relaymesh_pb2.ModelInfo(
            model_name="relaymesh-rl",
            model_version="0.1.0",
            last_trained=0,
            accuracy=0.0,
            is_loaded=self.inference.is_loaded,
        )


def serve(port: int = 50051):
    """Start the gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    relaymesh_pb2_grpc.add_AIServiceServicer_to_server(AIServicer(), server)
    
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    print(f"AI service started on port {port}")
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()

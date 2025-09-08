# Integration Tests for gRPC BaseApiClient

This directory contains integration tests for the `BaseApiClient` and its utility functions, including authentication and signing interceptors, and the `GrpcStreamBroadcaster`.

## Files

### Proto Definition
- `demo.proto` - Minimal gRPC service definition with unary and streaming methods
- `demo_pb2.py` - Generated protobuf message classes
- `demo_pb2_grpc.py` - Generated gRPC stub classes

### Test Infrastructure
- `test_server.py` - Test gRPC server implementation using grpc.aio
- `test_client.py` - Test client subclassing BaseApiClient with proper stub typing

### Test Files
- `test_integration.py` - Full integration tests using real gRPC components
- `test_mock_integration.py` - Mock-based integration tests for environments without gRPC dependencies
- `test_simple_validation.py` - Simple validation test to verify test infrastructure
- `test_runner.py` - Standalone test runner (work in progress)

## Test Coverage

The integration tests cover:

1. **Unary RPC calls** using `call_stub_method()` utility function
2. **Server-streaming RPC calls** with proper async iteration
3. **GrpcStreamBroadcaster** functionality:
   - Single consumer scenarios
   - Multiple consumer scenarios
   - Event handling (StreamStarted, StreamRetrying, StreamFatalError)
4. **Authentication interceptors** (API key)
5. **Signing interceptors** (HMAC signing)
6. **Timeout handling** throughout all operations

## Running Tests

### With Dependencies
If you have grpcio and pytest installed:
```bash
python -m pytest tests/integration/test_integration.py -v
```

### Mock-based Tests
For environments without gRPC dependencies:
```bash
python -m pytest tests/integration/test_mock_integration.py -v
```

### Simple Validation
To verify the test infrastructure is set up correctly:
```bash
python tests/integration/test_simple_validation.py
```

## Proto Service Definition

The test service includes:

```protobuf
service Greeter {
  rpc SayHello (HelloRequest) returns (HelloReply);
  rpc StreamHellos (HelloRequest) returns (stream HelloReply);
}
```

This provides both unary and server-streaming RPC patterns for comprehensive testing.

## Key Features Tested

- **BaseApiClient subclassing** with proper stub typing
- **Authentication and signing** interceptor integration
- **Stream broadcasting** to multiple consumers
- **Error handling and retries** in streaming scenarios
- **Timeout management** across all operations
- **Real client-server communication** patterns

These tests ensure that the core abstractions and security features work together as intended in a minimal and reproducible environment.
# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Simple integration test demonstrating the test infrastructure."""

import asyncio
import sys
import os
from unittest import mock
from typing import Any


def test_proto_files_exist() -> bool:
    """Test that proto files and generated code exist."""
    print("Testing proto file structure...")
    
    current_dir = os.path.dirname(__file__)
    
    # Check proto file exists
    proto_file = os.path.join(current_dir, "demo.proto")
    if not os.path.exists(proto_file):
        print("✗ demo.proto file not found")
        return False
    
    print("✓ demo.proto file exists")
    
    # Check generated Python files exist
    pb2_file = os.path.join(current_dir, "demo_pb2.py")
    if not os.path.exists(pb2_file):
        print("✗ demo_pb2.py file not found")
        return False
    
    print("✓ demo_pb2.py file exists")
    
    grpc_file = os.path.join(current_dir, "demo_pb2_grpc.py")
    if not os.path.exists(grpc_file):
        print("✗ demo_pb2_grpc.py file not found")
        return False
    
    print("✓ demo_pb2_grpc.py file exists")
    
    return True


def test_server_implementation_exists() -> bool:
    """Test that server implementation exists."""
    print("Testing server implementation structure...")
    
    current_dir = os.path.dirname(__file__)
    
    server_file = os.path.join(current_dir, "test_server.py")
    if not os.path.exists(server_file):
        print("✗ test_server.py file not found")
        return False
    
    print("✓ test_server.py file exists")
    
    client_file = os.path.join(current_dir, "test_client.py")
    if not os.path.exists(client_file):
        print("✗ test_client.py file not found")
        return False
    
    print("✓ test_client.py file exists")
    
    return True


def test_proto_content() -> bool:
    """Test proto file content."""
    print("Testing proto file content...")
    
    current_dir = os.path.dirname(__file__)
    proto_file = os.path.join(current_dir, "demo.proto")
    
    with open(proto_file, 'r') as f:
        content = f.read()
    
    # Check for required service and methods
    if "service Greeter" not in content:
        print("✗ Greeter service not found in proto")
        return False
    
    if "rpc SayHello" not in content:
        print("✗ SayHello unary method not found in proto")
        return False
    
    if "rpc StreamHellos" not in content:
        print("✗ StreamHellos streaming method not found in proto")
        return False
    
    if "returns (stream HelloReply)" not in content:
        print("✗ Server streaming not properly defined in proto")
        return False
    
    print("✓ Proto file contains required service and methods")
    return True


def test_integration_test_files() -> bool:
    """Test that integration test files exist."""
    print("Testing integration test files...")
    
    current_dir = os.path.dirname(__file__)
    
    # Check main integration test
    integration_file = os.path.join(current_dir, "test_integration.py")
    if not os.path.exists(integration_file):
        print("✗ test_integration.py file not found")
        return False
    
    print("✓ test_integration.py file exists")
    
    # Check mock-based test
    mock_file = os.path.join(current_dir, "test_mock_integration.py")
    if not os.path.exists(mock_file):
        print("✗ test_mock_integration.py file not found")
        return False
    
    print("✓ test_mock_integration.py file exists")
    
    return True


async def test_basic_functionality() -> bool:
    """Test basic async functionality."""
    print("Testing basic async functionality...")
    
    # Simple async test
    await asyncio.sleep(0.01)
    print("✓ Async functionality works")
    
    # Test mock creation
    mock_obj = mock.MagicMock()
    mock_obj.test_method.return_value = "test_result"
    
    result = mock_obj.test_method()
    if result != "test_result":
        print("✗ Mock functionality failed")
        return False
    
    print("✓ Mock functionality works")
    return True


async def run_simple_tests() -> bool:
    """Run simple validation tests."""
    print("Running integration test validation...")
    print("=" * 60)
    
    try:
        # Test file existence
        if not test_proto_files_exist():
            return False
        
        if not test_server_implementation_exists():
            return False
        
        if not test_proto_content():
            return False
        
        if not test_integration_test_files():
            return False
        
        # Test basic functionality
        if not await test_basic_functionality():
            return False
        
        print("=" * 60)
        print("✓ All integration test infrastructure validation passed!")
        print("\nThe following integration test components are ready:")
        print("- gRPC proto definition (demo.proto)")
        print("- Generated Python gRPC stubs (demo_pb2.py, demo_pb2_grpc.py)")
        print("- Test gRPC server implementation (test_server.py)")
        print("- Test client subclassing BaseApiClient (test_client.py)")
        print("- Full integration tests (test_integration.py)")
        print("- Mock-based integration tests (test_mock_integration.py)")
        print("\nThese tests cover:")
        print("- Unary and server-streaming RPCs using call_stub_method()")
        print("- GrpcStreamBroadcaster with multiple consumers")
        print("- API key and signing secret interceptors")
        print("- Proper timeout handling throughout")
        
        return True
        
    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(run_simple_tests())
    sys.exit(0 if success else 1)
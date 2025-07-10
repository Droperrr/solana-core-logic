#!/usr/bin/env python3
"""
End-to-end test for Solana transaction decoder service
"""
import json
import subprocess
import time
import requests
import sys
import os
from typing import Dict, Any

def load_real_transaction_data() -> Dict[str, Any]:
    """Load real transaction data from fixture file"""
    fixture_path = "tests/regression/fixtures/3TZ15bGjHmoUST4eHATy68XiDkjg3P7ct1xmknpSisNv3D5awF4GqHXTjV53Qxue1WLo4YrC5e3UaL4PNx4fuEok.json"
    
    if not os.path.exists(fixture_path):
        print(f"Error: Fixture file not found at {fixture_path}")
        sys.exit(1)
    
    try:
        with open(fixture_path, 'r') as f:
            raw_data = json.load(f)
        
        # Extract the actual transaction data from the RPC response
        if 'result' in raw_data:
            return {"raw_tx": raw_data['result']}
        else:
            return {"raw_tx": raw_data}
    except Exception as e:
        print(f"Error loading fixture data: {e}")
        sys.exit(1)

def run_command(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run shell command and return result"""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"Command failed: {result.stderr}")
        sys.exit(1)
    return result

def build_docker_image() -> str:
    """Build Docker image and return image name"""
    image_name = "solana-decoder:test"
    run_command(f"docker build -t {image_name} .")
    return image_name

def start_container(image_name: str) -> str:
    """Start container and return container ID"""
    container_id = run_command(
        f"docker run -d -p 8080:8080 --name decoder-test {image_name}"
    ).stdout.strip()
    
    # Wait for service to start
    print("Waiting for service to start...")
    time.sleep(10)
    
    # Wait for health check
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get("http://localhost:8080/health", timeout=5)
            if response.status_code == 200:
                print("Service is healthy!")
                break
        except requests.exceptions.RequestException:
            if i == max_retries - 1:
                print("Service failed to start")
                sys.exit(1)
            time.sleep(2)
    
    return container_id

def test_decode_endpoint() -> Dict[str, Any]:
    """Test /decode endpoint and return response"""
    url = "http://localhost:8080/decode"
    headers = {"Content-Type": "application/json"}
    
    # Load real transaction data
    test_data = load_real_transaction_data()
    
    print(f"Sending POST request to {url}")
    print(f"Request data size: {len(json.dumps(test_data))} characters")
    response = requests.post(url, json=test_data, headers=headers, timeout=30)
    
    if response.status_code != 200:
        print(f"Request failed with status {response.status_code}: {response.text}")
        sys.exit(1)
    
    return response.json()

def verify_response(response: Dict[str, Any]) -> bool:
    """Verify that response contains expected format"""
    print("Verifying response...")
    
    # Check for required fields according to API contract
    if "event" not in response:
        print("Response missing 'event' field")
        return False
    
    if "version" not in response:
        print("Response missing 'version' field")
        return False
    
    print(f"Response format is valid. Event type: {response.get('event', {}).get('type', 'unknown')}")
    return True

def cleanup(container_id: str):
    """Clean up container"""
    print("Cleaning up...")
    run_command(f"docker stop {container_id}", check=False)
    run_command(f"docker rm {container_id}", check=False)

def main():
    """Main test execution"""
    print("=== Solana Decoder E2E Test ===")
    
    try:
        # Build Docker image
        print("\n1. Building Docker image...")
        image_name = build_docker_image()
        
        # Start container
        print("\n2. Starting container...")
        container_id = start_container(image_name)
        
        # Test decode endpoint
        print("\n3. Testing /decode endpoint...")
        response = test_decode_endpoint()
        
        # Verify response
        print("\n4. Verifying response...")
        if verify_response(response):
            print("✅ E2E test PASSED!")
        else:
            print("❌ E2E test FAILED!")
            sys.exit(1)
        
        # Print final response
        print("\n5. Final JSON response:")
        print(json.dumps(response, indent=2))
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        sys.exit(1)
    finally:
        # Cleanup
        if 'container_id' in locals():
            cleanup(container_id)

if __name__ == "__main__":
    main() 
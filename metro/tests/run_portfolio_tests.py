#!/usr/bin/env python3
"""
Test runner script for the metro data pipeline.

This script demonstrates how to run different categories of tests
and is useful for CI/CD pipelines and portfolio demonstrations.
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(cmd, description):
    """Run a command and report results."""
    print(f"\n{'='*60}")
    print(f"🧪 {description}")
    print(f"{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print()
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print("✅ PASSED")
        if result.stdout:
            print("\nOutput:")
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print("❌ FAILED")
        if e.stdout:
            print("\nOutput:")
            print(e.stdout)
        if e.stderr:
            print("\nError:")
            print(e.stderr)
        return False

def main():
    """Run test suite with different configurations."""
    
    # Change to the metro directory
    script_dir = Path(__file__).parent
    metro_dir = script_dir.parent
    os.chdir(metro_dir)
    
    print("🚇 Metro Data Pipeline - Test Suite")
    print(f"Working directory: {metro_dir}")
    
    # Test categories to run
    test_scenarios = [
        {
            "cmd": ["python", "-m", "pytest", "tests/unit/test_bronze.py::TestBronze::test_get_train_patronage_success", "-v"],
            "description": "Bronze Layer - API Integration Test"
        },
        {
            "cmd": ["python", "-m", "pytest", "tests/unit/test_bronze.py::TestBronze::test_get_train_patronage_api_error", "-v"],
            "description": "Bronze Layer - Error Handling Test"
        },
        {
            "cmd": ["python", "-m", "pytest", "tests/unit/test_silver.py::TestSilver::test_extract_time_of_day_info", "-v"],
            "description": "Silver Layer - Time Processing Test"
        },
        {
            "cmd": ["python", "-m", "pytest", "tests/unit/test_silver.py::TestSilver::test_add_derived_date_parts", "-v"],
            "description": "Silver Layer - Date Processing Test"
        },
        {
            "cmd": ["python", "-m", "pytest", "tests/unit/test_base.py::TestBase::test_create_spark_success", "-v"],
            "description": "Infrastructure - Spark Session Test"
        },
    ]
    
    # Run tests
    passed = 0
    total = len(test_scenarios)
    
    for scenario in test_scenarios:
        if run_command(scenario["cmd"], scenario["description"]):
            passed += 1
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Passed: {passed}/{total}")
    print(f"Success Rate: {(passed/total)*100:.1f}%")
    
    if passed == total:
        print("🎉 All key tests passed! Your portfolio is ready.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the output above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

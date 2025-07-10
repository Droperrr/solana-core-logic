"""
CLI Template for QC Tools
Usage:
    python -m qc.cli_template --example-arg value
"""
import argparse
import logging

def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def main():
    """Main entry point for the CLI tool."""
    parser = argparse.ArgumentParser(description="QC CLI Tool Template.")
    parser.add_argument("--example-arg", type=str, required=True, help="Example argument.")
    args = parser.parse_args()
    setup_logging()
    logging.info(f"Received argument: {args.example_arg}")
    # TODO: Implement tool logic here

if __name__ == "__main__":
    main() 
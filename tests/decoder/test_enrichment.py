import json
import os
import pytest
from decoder import process_transaction
from decoder.enrichers import enrich_with_token_changes, enrich_with_compute_units
from decoder.schemas import EnrichedEvent

# Path to the fixtures
FIXTURES_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "fixtures")

def load_fixture(fixture_path):
    """Load a fixture from the given path."""
    with open(os.path.join(FIXTURES_PATH, fixture_path), "r") as f:
        return json.load(f)

def test_raydium_swap_enrichment():
    """Test the enrichment of a Raydium swap transaction."""
    # Load the fixture
    fixture = load_fixture("enrichment/raydium/happy_path_swap.json")
    
    # Process the transaction through the entire pipeline
    enriched_event = process_transaction(fixture)
    
    # Verify the enriched event structure
    assert isinstance(enriched_event, EnrichedEvent)
    
    # Check that token flows were calculated
    assert len(enriched_event.token_flows) > 0
    
    # Check that net token changes were calculated
    assert len(enriched_event.net_token_changes) > 0
    
    # Check that compute units were extracted
    assert enriched_event.compute_units_consumed is not None

def test_net_token_change_enricher():
    """Test the net token change enricher specifically."""
    # Load the fixture
    fixture = load_fixture("enrichment/raydium/happy_path_swap.json")
    
    # Create a mock resolved event
    mock_event = EnrichedEvent(
        event_id="test_id",
        tx_signature=fixture["transaction"]["signatures"][0],
        event_type="SWAP",
        program_id="675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        instruction_type="swapBaseIn",
        slot=fixture["slot"],
        block_time=fixture["blockTime"],
        initiator=fixture["transaction"]["message"]["accountKeys"][0],
        involved_accounts=fixture["transaction"]["message"]["accountKeys"]
    )
    
    # Apply the token change enricher
    enriched = enrich_with_token_changes(mock_event, fixture)
    
    # Verify that token flows were added
    assert len(enriched.token_flows) > 0
    
    # Verify that net token changes were calculated
    assert len(enriched.net_token_changes) > 0
    
    # Verify the structure of net token changes
    for wallet, token_changes in enriched.net_token_changes.items():
        assert isinstance(wallet, str)
        assert isinstance(token_changes, dict)
        for token_mint, amount in token_changes.items():
            assert isinstance(token_mint, str)
            assert isinstance(amount, int)

def test_compute_unit_enricher():
    """Test the compute unit enricher specifically."""
    # Load the fixture
    fixture = load_fixture("enrichment/raydium/happy_path_swap.json")
    
    # Create a mock resolved event
    mock_event = EnrichedEvent(
        event_id="test_id",
        tx_signature=fixture["transaction"]["signatures"][0],
        event_type="SWAP",
        program_id="675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        instruction_type="swapBaseIn",
        slot=fixture["slot"],
        block_time=fixture["blockTime"],
        initiator=fixture["transaction"]["message"]["accountKeys"][0],
        involved_accounts=fixture["transaction"]["message"]["accountKeys"]
    )
    
    # Apply the compute unit enricher
    enriched = enrich_with_compute_units(mock_event, fixture)
    
    # Verify that compute units were extracted
    assert enriched.compute_units_consumed is not None 
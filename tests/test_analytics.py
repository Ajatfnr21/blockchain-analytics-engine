"""Tests for Blockchain Analytics Engine"""

import pytest
from fastapi.testclient import TestClient
from decimal import Decimal

from src.main import app, blockchain_service, analytics_service, storage, Transaction

client = TestClient(app)


class TestHealth:
    def test_health_check(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "networks" in data

    def test_info(self):
        response = client.get("/")
        assert response.status_code == 200
        assert "Blockchain Analytics Engine" in response.json()["name"]


class TestBlockchain:
    def test_latest_block(self):
        response = client.get("/blockchain/ethereum/block/latest")
        assert response.status_code in [200, 503]  # 503 if network unavailable

    def test_balance_invalid_address(self):
        response = client.get("/blockchain/ethereum/balance/invalid")
        assert response.status_code == 400

    def test_get_transaction_not_found(self):
        response = client.get("/blockchain/ethereum/transaction/0x1234567890abcdef")
        assert response.status_code == 404


class TestAnalytics:
    def test_wallet_profile_empty(self):
        response = client.get("/analytics/wallet/0x742d35Cc6634C0532925a3b844Bc9e7595f8dEe")
        assert response.status_code == 200
        data = response.json()
        assert data["total_transactions"] == 0
        assert "new" in data["labels"]

    def test_wallet_transactions(self):
        # Add a sample transaction
        storage.add_transaction(Transaction(
            hash="0x123",
            from_address="0x742d35Cc6634C0532925a3b844Bc9e7595f8dEe",
            to_address="0x8ba1f109551bD432803012645Hac136c82C3e92C",
            value=Decimal("1.5"),
            gas_price=20000000000,
            gas_used=21000,
            timestamp=__import__('datetime').datetime.utcnow(),
            network="ethereum",
            block_number=1000000,
            input_data="0x",
            status="success"
        ))
        
        response = client.get("/analytics/wallet/0x742d35Cc6634C0532925a3b844Bc9e7595f8dEe/transactions")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] >= 1

    def test_network_stats(self):
        response = client.get("/analytics/network/ethereum/stats")
        assert response.status_code == 200
        data = response.json()
        assert "total_transactions_24h" in data
        assert "total_volume_24h" in data

    def test_top_wallets(self):
        response = client.get("/analytics/network/ethereum/top-wallets?limit=10")
        assert response.status_code == 200
        data = response.json()
        assert "wallets" in data

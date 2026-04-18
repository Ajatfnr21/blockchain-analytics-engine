#!/usr/bin/env python3
"""
Blockchain Analytics Engine - Real-time Blockchain Data Analytics

Features:
- Real-time transaction monitoring
- Wallet analytics & profiling
- Token transfer analysis
- DeFi protocol analytics
- Smart contract interaction tracking
- Whale movement alerts

Supports Ethereum, BSC, Polygon, Arbitrum networks.

Author: Drajat Sukma
License: MIT
Version: 2.0.0
"""

__version__ = "2.0.0"

import asyncio
import json
import os
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from contextlib import asynccontextmanager

import aiohttp
import websockets
from web3 import Web3
from eth_utils import to_checksum_address, from_wei
import pandas as pd
import structlog
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()

# ============== Configuration ==============

NETWORKS = {
    "ethereum": {
        "rpc": os.getenv("ETH_RPC", "https://eth.llamarpc.com"),
        "ws": os.getenv("ETH_WS", "wss://ethereum.publicnode.com"),
        "chain_id": 1,
        "native_token": "ETH"
    },
    "bsc": {
        "rpc": os.getenv("BSC_RPC", "https://bsc-dataseed.binance.org"),
        "ws": os.getenv("BSC_WS", "wss://bsc.publicnode.com"),
        "chain_id": 56,
        "native_token": "BNB"
    },
    "polygon": {
        "rpc": os.getenv("POLYGON_RPC", "https://polygon.llamarpc.com"),
        "ws": os.getenv("POLYGON_WS", "wss://polygon.publicnode.com"),
        "chain_id": 137,
        "native_token": "MATIC"
    },
    "arbitrum": {
        "rpc": os.getenv("ARB_RPC", "https://arb1.arbitrum.io/rpc"),
        "chain_id": 42161,
        "native_token": "ETH"
    }
}

# ERC20 ABI
ERC20_ABI = [
    {"constant": True, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
    {"anonymous": False, "inputs": [{"indexed": True, "name": "from", "type": "address"}, {"indexed": True, "name": "to", "type": "address"}, {"indexed": False, "name": "value", "type": "uint256"}], "name": "Transfer", "type": "event"}
]

# ============== Data Models ==============

@dataclass
class Transaction:
    hash: str
    from_address: str
    to_address: Optional[str]
    value: Decimal
    gas_price: int
    gas_used: int
    timestamp: datetime
    network: str
    block_number: int
    input_data: str
    status: str

@dataclass
class TokenTransfer:
    tx_hash: str
    token_address: str
    token_symbol: str
    from_address: str
    to_address: str
    value: Decimal
    timestamp: datetime
    network: str

@dataclass
class WalletProfile:
    address: str
    total_transactions: int
    total_volume_eth: Decimal
    first_seen: Optional[datetime]
    last_seen: Optional[datetime]
    tokens_held: List[Dict[str, Any]]
    risk_score: int  # 0-100
    labels: List[str]

class HealthResponse(BaseModel):
    status: str
    version: str
    networks: Dict[str, str]
    timestamp: datetime
    uptime_seconds: float

class TransactionQuery(BaseModel):
    network: str = "ethereum"
    start_block: Optional[int] = None
    end_block: Optional[int] = None
    address: Optional[str] = None
    limit: int = 100

class AnalyticsSummary(BaseModel):
    network: str
    total_transactions_24h: int
    total_volume_24h: float
    avg_gas_price: int
    active_addresses: int
    timestamp: datetime

# ============== In-Memory Storage ==============

class AnalyticsStorage:
    def __init__(self):
        self.transactions: List[Transaction] = []
        self.token_transfers: List[TokenTransfer] = []
        self.wallet_profiles: Dict[str, WalletProfile] = {}
        self.monitoring: Set[str] = set()  # Addresses being monitored
        self.alerts: List[Dict[str, Any]] = []
        self.analytics_cache: Dict[str, Any] = {}
        self.start_time = datetime.utcnow()
        
    def add_transaction(self, tx: Transaction):
        self.transactions.append(tx)
        # Keep only last 10000 transactions in memory
        if len(self.transactions) > 10000:
            self.transactions = self.transactions[-10000:]
    
    def get_wallet_transactions(self, address: str, network: str) -> List[Transaction]:
        return [
            tx for tx in self.transactions
            if tx.network == network and 
            (tx.from_address.lower() == address.lower() or 
             (tx.to_address and tx.to_address.lower() == address.lower()))
        ]

storage = AnalyticsStorage()

# ============== Blockchain Service ==============

class BlockchainService:
    """Core blockchain interaction service"""
    
    def __init__(self):
        self.connections: Dict[str, Web3] = {}
        self._init_connections()
    
    def _init_connections(self):
        for network, config in NETWORKS.items():
            try:
                w3 = Web3(Web3.HTTPProvider(config["rpc"]))
                if w3.is_connected():
                    self.connections[network] = w3
                    logger.info("network_connected", network=network)
                else:
                    logger.warning("network_connection_failed", network=network)
            except Exception as e:
                logger.error("network_connection_error", network=network, error=str(e))
    
    def get_balance(self, address: str, network: str = "ethereum") -> Decimal:
        if network not in self.connections:
            raise HTTPException(status_code=400, detail=f"Network {network} not available")
        
        w3 = self.connections[network]
        checksum_addr = to_checksum_address(address)
        balance_wei = w3.eth.get_balance(checksum_addr)
        return Decimal(from_wei(balance_wei, 'ether'))
    
    def get_transaction(self, tx_hash: str, network: str = "ethereum") -> Optional[Transaction]:
        if network not in self.connections:
            return None
        
        w3 = self.connections[network]
        try:
            tx_data = w3.eth.get_transaction(tx_hash)
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            
            return Transaction(
                hash=tx_hash,
                from_address=tx_data['from'],
                to_address=tx_data.get('to'),
                value=Decimal(from_wei(tx_data['value'], 'ether')),
                gas_price=tx_data.get('gasPrice', 0),
                gas_used=receipt['gasUsed'] if receipt else 0,
                timestamp=datetime.utcnow(),  # Would use block timestamp
                network=network,
                block_number=tx_data['blockNumber'],
                input_data=tx_data.get('input', '0x'),
                status="success" if receipt and receipt['status'] == 1 else "failed"
            )
        except Exception as e:
            logger.error("tx_fetch_error", tx_hash=tx_hash, error=str(e))
            return None
    
    def get_latest_block(self, network: str = "ethereum") -> Dict[str, Any]:
        if network not in self.connections:
            raise HTTPException(status_code=400, detail=f"Network {network} not available")
        
        w3 = self.connections[network]
        block = w3.eth.get_block('latest', full_transactions=False)
        return {
            "number": block.number,
            "hash": block.hash.hex(),
            "timestamp": datetime.fromtimestamp(block.timestamp).isoformat(),
            "transactions": len(block.transactions),
            "gas_used": block.gasUsed,
            "gas_limit": block.gasLimit
        }
    
    def analyze_contract(self, address: str, network: str = "ethereum") -> Dict[str, Any]:
        if network not in self.connections:
            return {}
        
        w3 = self.connections[network]
        checksum_addr = to_checksum_address(address)
        
        # Check if it's a contract
        code = w3.eth.get_code(checksum_addr)
        is_contract = len(code) > 0
        
        result = {
            "address": address,
            "is_contract": is_contract,
            "bytecode_size": len(code),
            "balance": float(self.get_balance(address, network))
        }
        
        # Try to get ERC20 info
        if is_contract:
            try:
                contract = w3.eth.contract(address=checksum_addr, abi=ERC20_ABI)
                result["token_name"] = contract.functions.name().call()
                result["token_symbol"] = contract.functions.symbol().call()
                result["decimals"] = contract.functions.decimals().call()
            except:
                pass
        
        return result
    
    async def scan_address_transactions(self, address: str, network: str = "ethereum", 
                                       blocks: int = 100) -> List[Transaction]:
        """Scan recent transactions for an address"""
        if network not in self.connections:
            return []
        
        w3 = self.connections[network]
        latest = w3.eth.block_number
        transactions = []
        
        for block_num in range(latest - blocks, latest + 1):
            try:
                block = w3.eth.get_block(block_num, full_transactions=True)
                for tx in block.transactions:
                    if (tx['from'].lower() == address.lower() or 
                        (tx.get('to') and tx['to'].lower() == address.lower())):
                        transactions.append(Transaction(
                            hash=tx['hash'].hex(),
                            from_address=tx['from'],
                            to_address=tx.get('to'),
                            value=Decimal(from_wei(tx['value'], 'ether')),
                            gas_price=tx.get('gasPrice', 0),
                            gas_used=0,
                            timestamp=datetime.fromtimestamp(block.timestamp),
                            network=network,
                            block_number=block_num,
                            input_data=tx.get('input', '0x'),
                            status="pending"
                        ))
            except Exception as e:
                logger.error("block_scan_error", block=block_num, error=str(e))
                continue
        
        return transactions

# ============== Analytics Service ==============

class AnalyticsService:
    """Analytics and reporting service"""
    
    @staticmethod
    def calculate_wallet_profile(address: str, network: str = "ethereum") -> WalletProfile:
        transactions = storage.get_wallet_transactions(address, network)
        
        if not transactions:
            return WalletProfile(
                address=address,
                total_transactions=0,
                total_volume_eth=Decimal("0"),
                first_seen=None,
                last_seen=None,
                tokens_held=[],
                risk_score=0,
                labels=["new"]
            )
        
        total_volume = sum(tx.value for tx in transactions)
        
        # Calculate risk score
        risk_score = 0
        labels = []
        
        if len(transactions) > 1000:
            labels.append("active")
        if total_volume > Decimal("1000"):
            labels.append("whale")
            risk_score += 20
        if len(transactions) < 5:
            risk_score += 10
            labels.append("new")
        
        return WalletProfile(
            address=address,
            total_transactions=len(transactions),
            total_volume_eth=total_volume,
            first_seen=min(tx.timestamp for tx in transactions),
            last_seen=max(tx.timestamp for tx in transactions),
            tokens_held=[],  # Would fetch from token balance APIs
            risk_score=min(risk_score, 100),
            labels=labels
        )
    
    @staticmethod
    def get_network_stats(network: str = "ethereum") -> AnalyticsSummary:
        # Get transactions from last 24 hours (simulated)
        recent_txs = [
            tx for tx in storage.transactions
            if tx.network == network and 
            tx.timestamp > datetime.utcnow() - timedelta(hours=24)
        ]
        
        addresses = set()
        total_volume = Decimal("0")
        
        for tx in recent_txs:
            addresses.add(tx.from_address.lower())
            if tx.to_address:
                addresses.add(tx.to_address.lower())
            total_volume += tx.value
        
        avg_gas = sum(tx.gas_price for tx in recent_txs) / len(recent_txs) if recent_txs else 0
        
        return AnalyticsSummary(
            network=network,
            total_transactions_24h=len(recent_txs),
            total_volume_24h=float(total_volume),
            avg_gas_price=int(avg_gas),
            active_addresses=len(addresses),
            timestamp=datetime.utcnow()
        )

# ============== WebSocket Monitor ==============

class WebSocketMonitor:
    """Real-time blockchain monitoring via WebSocket"""
    
    def __init__(self):
        self.connections: Dict[str, websockets.WebSocketClientProtocol] = {}
        self.running = False
    
    async def start_monitoring(self, network: str = "ethereum"):
        """Start monitoring a network for new transactions"""
        config = NETWORKS.get(network)
        if not config or not config.get("ws"):
            logger.warning("no_ws_available", network=network)
            return
        
        try:
            async with websockets.connect(config["ws"]) as ws:
                self.connections[network] = ws
                logger.info("ws_monitor_started", network=network)
                
                while self.running:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30)
                        data = json.loads(message)
                        await self._process_message(data, network)
                    except asyncio.TimeoutError:
                        await ws.ping()
                    except Exception as e:
                        logger.error("ws_message_error", error=str(e))
        except Exception as e:
            logger.error("ws_connection_error", network=network, error=str(e))
    
    async def _process_message(self, data: Dict, network: str):
        """Process incoming WebSocket message"""
        # This would handle new pending transactions, blocks, etc.
        logger.debug("ws_message_received", network=network, method=data.get("method"))

blockchain_service = BlockchainService()
analytics_service = AnalyticsService()
ws_monitor = WebSocketMonitor()

# ============== FastAPI Application ==============

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("blockchain_analytics_starting", version=__version__)
    ws_monitor.running = True
    yield
    ws_monitor.running = False
    logger.info("blockchain_analytics_stopping")

app = FastAPI(
    title="Blockchain Analytics Engine",
    version=__version__,
    description="Real-time Blockchain Data Analytics",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============== API Endpoints ==============

@app.get("/health", response_model=HealthResponse)
def health_check():
    uptime = (datetime.utcnow() - storage.start_time).total_seconds()
    return HealthResponse(
        status="healthy",
        version=__version__,
        networks={n: "connected" if n in blockchain_service.connections else "disconnected" 
                 for n in NETWORKS.keys()},
        timestamp=datetime.utcnow(),
        uptime_seconds=uptime
    )

@app.get("/")
def info():
    return {
        "name": "Blockchain Analytics Engine",
        "version": __version__,
        "networks": list(NETWORKS.keys()),
        "features": [
            "Real-time transaction monitoring",
            "Wallet analytics & profiling",
            "Token transfer tracking",
            "Smart contract analysis",
            "Network statistics"
        ]
    }

# Blockchain Endpoints

@app.get("/blockchain/{network}/block/latest")
def get_latest_block(network: str):
    return blockchain_service.get_latest_block(network)

@app.get("/blockchain/{network}/balance/{address}")
def get_balance(network: str, address: str):
    balance = blockchain_service.get_balance(address, network)
    return {
        "address": address,
        "network": network,
        "balance": float(balance),
        "currency": NETWORKS[network]["native_token"]
    }

@app.get("/blockchain/{network}/transaction/{tx_hash}")
def get_transaction(network: str, tx_hash: str):
    tx = blockchain_service.get_transaction(tx_hash, network)
    if not tx:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return {
        "hash": tx.hash,
        "from": tx.from_address,
        "to": tx.to_address,
        "value": float(tx.value),
        "gas_price": tx.gas_price,
        "block_number": tx.block_number,
        "status": tx.status
    }

@app.get("/blockchain/{network}/contract/{address}")
def analyze_contract(network: str, address: str):
    return blockchain_service.analyze_contract(address, network)

# Wallet Analytics

@app.get("/analytics/wallet/{address}")
async def get_wallet_profile(address: str, network: str = "ethereum"):
    # First scan for recent transactions
    await blockchain_service.scan_address_transactions(address, network)
    profile = analytics_service.calculate_wallet_profile(address, network)
    return asdict(profile)

@app.get("/analytics/wallet/{address}/transactions")
def get_wallet_transactions(address: str, network: str = "ethereum", limit: int = 50):
    txs = storage.get_wallet_transactions(address, network)
    return {
        "address": address,
        "count": len(txs),
        "transactions": [
            {
                "hash": tx.hash,
                "from": tx.from_address,
                "to": tx.to_address,
                "value": float(tx.value),
                "block_number": tx.block_number,
                "timestamp": tx.timestamp.isoformat(),
                "status": tx.status
            }
            for tx in txs[-limit:]
        ]
    }

# Network Analytics

@app.get("/analytics/network/{network}/stats")
def get_network_stats(network: str):
    return analytics_service.get_network_stats(network).model_dump()

@app.get("/analytics/network/{network}/top-wallets")
def get_top_wallets(network: str = "ethereum", limit: int = 20):
    """Get top wallets by transaction volume"""
    wallet_volumes: Dict[str, Decimal] = {}
    
    for tx in storage.transactions:
        if tx.network == network:
            wallet_volumes[tx.from_address] = wallet_volumes.get(tx.from_address, Decimal("0")) + tx.value
            if tx.to_address:
                wallet_volumes[tx.to_address] = wallet_volumes.get(tx.to_address, Decimal("0")) + tx.value
    
    sorted_wallets = sorted(wallet_volumes.items(), key=lambda x: x[1], reverse=True)[:limit]
    
    return {
        "network": network,
        "wallets": [
            {"address": addr, "volume": float(vol), "rank": i+1}
            for i, (addr, vol) in enumerate(sorted_wallets)
        ]
    }

# ============== CLI Interface ==============

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Blockchain Analytics Engine")
    parser.add_argument("command", choices=["serve", "scan", "balance", "tx"], help="Command")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--network", default="ethereum", choices=list(NETWORKS.keys()))
    parser.add_argument("--address", help="Wallet address")
    parser.add_argument("--tx-hash", help="Transaction hash")
    
    args = parser.parse_args()
    
    if args.command == "serve":
        uvicorn.run(app, host=args.host, port=args.port)
    elif args.command == "scan":
        if not args.address:
            print("Error: --address required for scan")
        else:
            print(f"Scanning {args.address} on {args.network}...")
            result = asyncio.run(blockchain_service.scan_address_transactions(
                args.address, args.network
            ))
            print(f"Found {len(result)} transactions")
    elif args.command == "balance":
        if not args.address:
            print("Error: --address required")
        else:
            balance = blockchain_service.get_balance(args.address, args.network)
            print(f"Balance: {balance} {NETWORKS[args.network]['native_token']}")
    elif args.command == "tx":
        if not args.tx_hash:
            print("Error: --tx-hash required")
        else:
            tx = blockchain_service.get_transaction(args.tx_hash, args.network)
            if tx:
                print(f"Transaction: {tx.hash}")
                print(f"From: {tx.from_address}")
                print(f"To: {tx.to_address}")
                print(f"Value: {tx.value} {NETWORKS[args.network]['native_token']}")
            else:
                print("Transaction not found")

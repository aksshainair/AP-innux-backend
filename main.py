"""
Enhanced FastAPI Backend for Reconciliation Dashboard
Combines your business logic with real-time MongoDB Atlas integration
"""
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient  # Async MongoDB driver
from pydantic import BaseModel, field_validator, Field, BeforeValidator
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Annotated
import os
from dotenv import load_dotenv
import json
import asyncio
import logging
from langgraph.graph import START, END

# --- NEW SIMPLIFIED INTEGRATION ---
from simplified_integration import (
    SimplifiedBusinessRulesEngine,
    SimplifiedWorkflowEngine,
    SimplifiedHumanReviewManager,
    create_sample_rules,
    SimplifiedWorkflowState,
    SimplifiedWorkflowBuilder,
    RuleAction,
    ReviewPriority
)

# --- OLD INTEGRATION ---
# from human_review_integration import (
#     EnhancedBusinessRulesEngine,
# )

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Reconciliation Dashboard API", version="1.0.0")

# Custom type for ObjectId conversion
PyObjectId = Annotated[str, BeforeValidator(str)]

# CORS middleware for React frontend
app.add_middleware(
    CORSMiddleware,
    # Allow localhost for development.
    # In production, you would use os.getenv("FRONTEND_URL")
    allow_origins=[os.getenv("FRONTEND_URL"), "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB Atlas connection
# For local development, ensure your .env file sets MONGODB_ATLAS_URI
# to your local MongoDB instance (e.g., "mongodb://localhost:27017")
MONGODB_URL = os.getenv("MONGODB_ATLAS_URI") or os.getenv("MONGO_CONNECTION")
DATABASE_NAME = os.getenv("DB_NAME", "accounting_db1") or os.getenv("MONGO_DATABASE", "accounting_db1")

# Global variables for MongoDB
mongodb_client = None
db = None

# Global variables for Human Review System
rules_engine = None
workflow_engine = None
review_manager = None

# Pydantic Models (from your original file + enhancements)
class KPIMetrics(BaseModel):
    total_pos: int
    total_invoices: int
    matched_invoices: int
    unmatched_invoices: int
    total_po_value: float
    total_invoice_value: float

class ChartDataPoint(BaseModel):
    date: str
    value: float
    count: int

class SupplierValue(BaseModel):
    supplier: str
    value: float
    count: int

class DepartmentData(BaseModel):
    department: str
    value: float
    percentage: float

class LocationData(BaseModel):
    location: str
    lat: float
    lng: float
    po_count: int
    total_value: float

class VendorDetails(BaseModel):
    location: str
    vendor: str
    contracts: int
    total_value: float

class VendorStat(BaseModel):
    vendor: str
    matched: int
    unmatched: int

class InvoiceRawFields(BaseModel):
    invoice_number: Optional[str] = None
    po_number: Optional[str] = None
    vendor_name: Optional[str] = None
    amount_due: Optional[float] = None

    @field_validator('amount_due', mode='before')
    def clean_number(cls, v):
        if v is None: return None
        if isinstance(v, str): v = v.replace(',', '')
        try: return float(v)
        except (ValueError, TypeError): return None

class InvoiceData(BaseModel):
    id: PyObjectId = Field(..., alias='_id')
    raw_fields: InvoiceRawFields
    match_status: Optional[str] = None
    class Config:
        populate_by_name = True

class PoRawFields(BaseModel):
    po_number: Optional[str] = None
    vendor_name: Optional[str] = None

class POData(BaseModel):
    id: PyObjectId = Field(..., alias='_id')
    raw_fields: PoRawFields
    po_date: Optional[datetime] = None
    status: Optional[str] = None
    total_amount: Optional[float] = None

    @field_validator('total_amount', mode='before')
    def clean_number(cls, v):
        if v is None: return None
        if isinstance(v, str): v = v.replace(',', '')
        try: return float(v)
        except (ValueError, TypeError): return None
    class Config:
        populate_by_name = True

class ActivityItem(BaseModel):
    timestamp: datetime
    type: str
    description: str
    amount: Optional[float] = None
    @field_validator('amount', mode='before')
    def clean_number(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            v = v.replace(',', '')
        return float(v)

class DashboardStats(BaseModel):
    timestamp: str
    kpis: KPIMetrics
    po_timeline: List[ChartDataPoint]
    invoice_timeline: List[ChartDataPoint]
    supplier_values: List[SupplierValue]
    department_distribution: List[DepartmentData]
    location_data: List[LocationData]
    vendor_details: List[VendorDetails]
    recent_activity: List[ActivityItem]

# WebSocket Manager for real-time updates
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        if not self.active_connections:
            return

        message_str = json.dumps(message, default=str)
        disconnected = []
        
        for connection in self.active_connections:
            try:
                await connection.send_text(message_str)
            except Exception as e:
                logger.error(f"Error sending to WebSocket: {e}")
                disconnected.append(connection)

        # Remove disconnected clients
        for connection in disconnected:
            self.disconnect(connection)

manager = ConnectionManager()

# Helper Functions
def get_percentage_change(current: float, previous: float) -> float:
    """Calculate percentage change"""
    if previous == 0:
        return 0.0
    return ((current - previous) / previous) * 100
def get_date_range(days: int = 28) -> tuple:
    """Get date range for timeline charts"""
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)
    return start_date, end_date

# Database initialization
async def init_database():
    """Initialize MongoDB connection"""
    global mongodb_client, db
    try:
        mongodb_client = AsyncIOMotorClient(MONGODB_URL)
        db = mongodb_client[DATABASE_NAME]
        # Test connection
        await mongodb_client.admin.command('ping')
        logger.info(f"✅ Connected to MongoDB Atlas: {DATABASE_NAME}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to connect to MongoDB: {e}")
        return False

async def close_database():
    """Close MongoDB connection"""
    global mongodb_client
    if mongodb_client:
        mongodb_client.close()
        logger.info("🔌 MongoDB connection closed")

# Collection references
def get_collections():
    """Get collection references"""
    global db
    return {
        'po_collection': db.po_collection,
        'invoice_collection': db.invoice_collection,
        'matching_collection': db.matching_result_collection,
        'vendors_collection': db.vendors if hasattr(db, 'vendors') else db.po_collection
    }

# API Endpoints
@app.get("/")
async def root():
    return {"message": "Enhanced Reconciliation Dashboard API", "status": "running"}

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    try:
        await mongodb_client.admin.command('ping')
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@app.get("/api/metrics/kpis", response_model=KPIMetrics)
async def get_kpi_metrics():
    """Get main KPI metrics for dashboard cards"""
    try:
        # Direct collection access
        po_collection = db.po_collection
        invoice_collection = db.invoice_collection
        matching_collection = db.matching_result_collection

        # Current metrics
        total_pos = await po_collection.count_documents({})
        total_invoices = await invoice_collection.count_documents({})
        
        # Count matched/unmatched based on matching_result_collection
        matched_count = await matching_collection.count_documents({"status": {"$in": ["match", "matched_cumulative", "matched_with_tolerance", "matched"]}})
        total_matches = await matching_collection.count_documents({})
        unmatched_count = total_matches - matched_count
        
        # If no matching results, fall back to invoice match_status
        if total_matches == 0:
            matched_invoices = await invoice_collection.count_documents({"match_status": {"$in": ["matched", "matched_cumulative", "matched_with_tolerance", "matched"]}})
            unmatched_invoices = total_invoices - matched_invoices
        else:
            matched_invoices = matched_count
            unmatched_invoices = unmatched_count

        def parse_amount(value):
            if isinstance(value, str):
                return float(value.replace(',', ''))
            try:
                return float(value)
            except Exception:
                return 0.0

        # Fetch all relevant documents
        po_docs = await po_collection.find({}, {"raw_fields.total_amount": 1}).to_list(None)
        invoice_docs = await invoice_collection.find({}, {"raw_fields.amount_due": 1}).to_list(None)
        
        total_po_value = sum(parse_amount(doc.get("raw_fields", {}).get("total_amount", 0)) for doc in po_docs)
        total_invoice_value = sum(parse_amount(doc.get("raw_fields", {}).get("amount_due", 0)) for doc in invoice_docs)   
        
        # # Value aggregations
        # po_pipeline = [{"$group": {"_id": None, "total": {"$sum": "$total_amount"}}}]
        # invoice_pipeline = [{"$group": {"_id": None, "total": {"$sum": "$raw_fields.total_sum"}}}]
        
        # po_value_result = await po_collection.aggregate(po_pipeline).to_list(1)
        # invoice_value_result = await invoice_collection.aggregate(invoice_pipeline).to_list(1)
        
        # total_po_value = po_value_result[0]["total"] if po_value_result else 0.0
        # total_invoice_value = invoice_value_result[0]["total"] if invoice_value_result else 0.0
        
        return KPIMetrics(
            total_pos=total_pos,
            total_invoices=total_invoices,
            matched_invoices=matched_invoices,
            unmatched_invoices=unmatched_invoices,
            total_po_value=total_po_value,
            total_invoice_value=total_invoice_value
        )
        
    except Exception as e:
        logger.error(f"Error fetching KPI metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching KPI metrics: {str(e)}")

@app.get("/api/charts/po-timeline", response_model=List[ChartDataPoint])
async def get_po_timeline():
    """Get PO timeline data for last 4 weeks"""
    try:
        po_collection = db.po_collection
        
        start_date, end_date = get_date_range(28)
        
        pipeline = [
            {
                "$match": {
                    "created_at": {"$gte": start_date, "$lte": end_date}
                }
            },
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$created_at"
                        }
                    },
                    "count": {"$sum": 1},
                    "value": {"$sum": "$total_amount"}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        results = await po_collection.aggregate(pipeline).to_list(100)
        
        # Fill missing dates with zero values
        timeline_data = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            found_data = next((r for r in results if r["_id"] == date_str), None)
            
            if found_data:
                timeline_data.append(ChartDataPoint(
                    date=date_str,
                    value=found_data["value"],
                    count=found_data["count"]
                ))
            else:
                timeline_data.append(ChartDataPoint(
                    date=date_str,
                    value=0.0,
                    count=0
                ))
            
            current_date += timedelta(days=1)
        
        return timeline_data
        
    except Exception as e:
        logger.error(f"Error fetching PO timeline: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching PO timeline: {str(e)}")

@app.get("/api/charts/invoice-timeline", response_model=List[ChartDataPoint])
async def get_invoice_timeline():
    """Get Invoice timeline data for last 4 weeks"""
    try:
        collections = get_collections()
        invoice_collection = collections['invoice_collection']
        
        start_date, end_date = get_date_range(28)
        
        pipeline = [
            {
                "$match": {
                    "created_at": {"$gte": start_date, "$lte": end_date}
                }
            },
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$created_at"
                        }
                    },
                    "count": {"$sum": 1},
                    "value": {"$sum": "$total_amount"}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        results = await invoice_collection.aggregate(pipeline).to_list(100)
        
        # Fill missing dates
        timeline_data = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            found_data = next((r for r in results if r["_id"] == date_str), None)
            
            if found_data:
                timeline_data.append(ChartDataPoint(
                    date=date_str,
                    value=found_data["value"],
                    count=found_data["count"]
                ))
            else:
                timeline_data.append(ChartDataPoint(
                    date=date_str,
                    value=0.0,
                    count=0
                ))
            
            current_date += timedelta(days=1)
        
        return timeline_data
        
    except Exception as e:
        logger.error(f"Error fetching Invoice timeline: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching Invoice timeline: {str(e)}")

@app.get("/api/charts/supplier-values", response_model=List[SupplierValue])
async def get_supplier_values():
    """Get total invoice value by supplier for bar chart"""
    try:
        collections = get_collections()
        invoice_collection = collections['invoice_collection']
        
        # Try lookup with vendors collection first, fallback to vendor_name field
        pipeline = [
            {
                "$group": {
                    "_id": "$vendor_name",  # Use vendor_name field directly
                    "total_value": {"$sum": "$total_amount"},
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"total_value": -1}},
            {"$limit": 10}
        ]
        
        results = await invoice_collection.aggregate(pipeline).to_list(10)
        
        return [
            SupplierValue(
                supplier=result["_id"] or "Unknown",
                value=result["total_value"],
                count=result["count"]
            )
            for result in results
        ]
        
    except Exception as e:
        logger.error(f"Error fetching supplier values: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching supplier values: {str(e)}")

@app.get("/api/charts/vendor-stats", response_model=List[VendorStat])
async def get_vendor_stats():
    """Get matched/unmatched invoice counts by vendor"""
    try:
        invoice_collection = db.invoice_collection
        
        pipeline = [
            {
                "$group": {
                    "_id": "$raw_fields.vendor_name",
                    "matched": {
                        "$sum": {
                            "$cond": [ { "$in": [ "$match_status", ["matched_cumulative", "matched"] ] }, 1, 0 ]
                        }
                    },
                    "unmatched": {
                        "$sum": {
                            "$cond": [ { "$not": { "$in": [ "$match_status", ["matched_cumulative", "matched"] ] } }, 1, 0 ]
                        }
                    }
                }
            },
            {
                "$project": {
                    "vendor": "$_id",
                    "matched": "$matched",
                    "unmatched": "$unmatched",
                    "_id": 0
                }
            },
            {"$sort": {"vendor": 1}}
        ]
        
        results = await invoice_collection.aggregate(pipeline).to_list(None)
        return [item for item in results if item['vendor']]
        
    except Exception as e:
        logger.error(f"Error fetching vendor stats: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching vendor stats: {str(e)}")

@app.get("/api/invoices", response_model=List[InvoiceData])
async def get_invoices_data():
    """Get all invoices"""
    try:
        invoices_cursor = db.invoice_collection.find({}).limit(100)
        return await invoices_cursor.to_list(length=100)
    except Exception as e:
        logger.error(f"Error fetching invoices: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching invoices: {str(e)}")

@app.get("/api/purchase-orders", response_model=List[POData])
async def get_pos_data():
    """Get all purchase orders"""
    try:
        pos_cursor = db.po_collection.find({}).limit(100)
        return await pos_cursor.to_list(length=100)
    except Exception as e:
        logger.error(f"Error fetching purchase orders: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching purchase orders: {str(e)}")

@app.get("/api/activity-feed", response_model=List[ActivityItem])
async def get_recent_activity():
    """Get recent processing activity"""
    try:
        collections = get_collections()
        po_collection = collections['po_collection']
        invoice_collection = collections['invoice_collection']
        matching_collection = collections['matching_collection']
        
        # Get recent activities
        recent_pos = await po_collection.find(
            {}, {"po_number": 1, "total_amount": 1, "created_at": 1}
        ).sort("created_at", -1).limit(5).to_list(5)
        
        recent_invoices = await invoice_collection.find(
            {}, {"invoice_number": 1, "total_amount": 1, "created_at": 1}
        ).sort("created_at", -1).limit(5).to_list(5)
        
        recent_matches = await matching_collection.find(
            {}, {"po_number": 1, "invoice_number": 1, "created_at": 1, "status": 1}
        ).sort("created_at", -1).limit(5).to_list(5)
        
        activities = []
        
        # Add activities
        for po in recent_pos:
            activities.append(ActivityItem(
                timestamp=po.get("created_at", datetime.now(timezone.utc)),
                type="po_created",
                description=f"PO {po.get('po_number', 'Unknown')} created",
                amount=po.get("total_amount")
            ))
        
        for invoice in recent_invoices:
            activities.append(ActivityItem(
                timestamp=invoice.get("created_at", datetime.now(timezone.utc)),
                type="invoice_received",
                description=f"Invoice {invoice.get('invoice_number', 'Unknown')} received",
                amount=invoice.get("total_amount")
            ))
        
        for match in recent_matches:
            match_type = "match_found" if match.get("status") == "match" else "discrepancy_found"
            activities.append(ActivityItem(
                timestamp=match.get("created_at", datetime.now(timezone.utc)),
                type=match_type,
                description=f"{'Match' if match.get('status') == 'match' else 'Discrepancy'} found: {match.get('po_number', 'Unknown')} ↔ {match.get('invoice_number', 'Unknown')}"
            ))
        
        # Sort by timestamp and return latest 10
        activities.sort(key=lambda x: x.timestamp, reverse=True)
        return activities[:10]
        
    except Exception as e:
        logger.error(f"Error fetching activity feed: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching activity feed: {str(e)}")

@app.get("/api/dashboard/stats", response_model=DashboardStats)
async def get_complete_dashboard_stats():
    # This function now acts as a high-level orchestrator for fetching all data
    # It can be called by the frontend to get a complete snapshot
    try:
        kpis, po_timeline, invoice_timeline, supplier_values, vendor_stats, invoices, pos, activity = await asyncio.gather(
            get_kpi_metrics(),
            get_po_timeline(),
            get_invoice_timeline(),
            get_supplier_values(),
            get_vendor_stats(),
            get_invoices_data(),
            get_pos_data(),
            get_recent_activity()
        )

        # Dummy data for components not yet fully implemented from scratch
        department_distribution = [DepartmentData(department='Sales', value=120000, percentage=40.0), DepartmentData(department='R&D', value=180000, percentage=60.0)]
        location_data = [LocationData(location='New York', lat=40.7128, lng=-74.0060, po_count=15, total_value=150000)]
        vendor_details = [VendorDetails(location='New York', vendor='Tech Corp', contracts=5, total_value=75000)]

        return DashboardStats(
            timestamp=datetime.now().isoformat(),
            kpis=kpis,
            po_timeline=po_timeline,
            invoice_timeline=invoice_timeline,
            supplier_values=supplier_values,
            department_distribution=department_distribution,
            location_data=location_data,
            vendor_details=vendor_details,
            recent_activity=activity
        )
    except Exception as e:
        logger.error(f"Error fetching complete dashboard stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch dashboard statistics.")

async def define_invoice_processing_workflow(engine, rules_engine, review_manager):
    """Defines the invoice processing workflow using the simplified builder."""
    builder = SimplifiedWorkflowBuilder(engine)
    
    # Define workflow nodes (simplified)
    async def extract_invoice_data(state: SimplifiedWorkflowState) -> SimplifiedWorkflowState:
        # In a real scenario, this would involve OCR or parsing
        state['po_number'] = "PO-001" 
        state['extracted_invoice_data'] = {"total_amount": 1200, "vendor": "GLOBAL_TECH"}
        return state

    async def fetch_po_data(state: SimplifiedWorkflowState) -> SimplifiedWorkflowState:
        state['po_data'] = {"total_amount": 1000, "vendor": "GLOBAL_TECH"}
        return state

    async def match_po_invoice(state: SimplifiedWorkflowState) -> SimplifiedWorkflowState:
        po_total = state.get("po_data", {}).get("total_amount", 0)
        inv_total = state.get("extracted_invoice_data", {}).get("total_amount", 0)
        variance = abs(po_total - inv_total)
        state['matching_result'] = {
            "total_variance": variance,
            "total_variance_percentage": (variance / po_total) * 100 if po_total else 0,
            "has_discrepancy": variance > 0
        }
        return state

    async def apply_business_rules(state: SimplifiedWorkflowState) -> SimplifiedWorkflowState:
        context = {
            "po_total": state.get("po_data", {}).get("total_amount", 0),
            "vendor_name": state.get("po_data", {}).get("vendor", "Unknown"),
            "total_variance_percentage": state.get("matching_result", {}).get("total_variance_percentage", 0),
            "has_discrepancy": state.get("matching_result", {}).get("has_discrepancy", False)
        }
        # Use a generic rule type for this example
        result = await rules_engine.evaluate_rules(context, "discrepancy")
        
        state['human_review_required'] = result.final_decision == RuleAction.REQUIRE_REVIEW
        state['final_decision'] = result.final_decision.value if result.final_decision else "No Decision"
        state['final_reasoning'] = "; ".join(result.messages)
        return state

    async def needs_human_review(state: SimplifiedWorkflowState) -> str:
        return "human_review_node" if state.get('human_review_required') else END

    async def human_review_node(state: SimplifiedWorkflowState) -> SimplifiedWorkflowState:
        # This node would typically wait for human input.
        # For this simplified model, we'll just log that it's waiting.
        logger.info(f"Workflow {state['workflow_id']} is waiting for human review.")
        state['metadata']['status'] = 'WAITING_FOR_INPUT'
        # The workflow pauses here until a human submits a review via the API.
        return state
        
    # Build the graph
    builder.add_node("extract_invoice_data", extract_invoice_data)
    builder.add_node("fetch_po_data", fetch_po_data)
    builder.add_node("match_po_invoice", match_po_invoice)
    builder.add_node("apply_business_rules", apply_business_rules)
    builder.add_node("human_review_node", human_review_node)
    
    builder.add_edge(START, "extract_invoice_data")
    builder.add_edge("extract_invoice_data", "fetch_po_data")
    builder.add_edge("fetch_po_data", "match_po_invoice")
    builder.add_edge("match_po_invoice", "apply_business_rules")
    
    builder.add_conditional_edge("apply_business_rules", needs_human_review, {
        "human_review_node": "human_review_node",
        END: END
    })
    
    return builder.build("invoice_processing")

# --- Human Review API Endpoints ---
class ReviewSubmission(BaseModel):
    decision: str
    reasoning: str
    reviewer_id: str
    new_rule: Optional[str] = None

@app.get("/api/reviews/pending")
async def get_pending_reviews_endpoint():
    """Get all pending human review requests."""
    if not review_manager:
        raise HTTPException(status_code=503, detail="Review system not initialized")
    try:
        pending_reviews = await review_manager.get_pending_reviews()
        return pending_reviews
    except Exception as e:
        logger.error(f"Error fetching pending reviews: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch pending reviews.")

@app.post("/api/reviews/{request_id}/submit")
async def submit_review_endpoint(request_id: str, submission: ReviewSubmission):
    """Submit a decision for a human review request."""
    if not review_manager:
        raise HTTPException(status_code=503, detail="Review system not initialized")
    try:
        success = await review_manager.submit_review_response(
            request_id=request_id,
            reviewer_id=submission.reviewer_id,
            decision=submission.decision,
            reasoning=submission.reasoning,
            new_rule=submission.new_rule
        )
        if not success:
            raise HTTPException(status_code=404, detail="Review request not found or failed to submit.")
        
        # Optionally, resume the workflow here if the design requires it.
        # For simplicity, we assume the workflow is either terminal or handled elsewhere.

        return {"status": "success", "message": "Review submitted successfully."}
    except Exception as e:
        logger.error(f"Error submitting review {request_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to submit review.")

@app.post("/api/workflows/trigger-test")
async def trigger_test_workflow():
    """Triggers a test invoice processing workflow."""
    if not workflow_engine:
        raise HTTPException(status_code=503, detail="Workflow engine not initialized")
    
    try:
        initial_state = {"invoice_text": "Sample invoice text for testing."}
        workflow_id = await workflow_engine.start_workflow("invoice_processing", initial_state)
        
        # Asynchronously execute the workflow
        asyncio.create_task(workflow_engine.execute_workflow(workflow_id))
        
        return {"message": "Test workflow triggered successfully", "workflow_id": workflow_id}
    except Exception as e:
        logger.error(f"Error triggering test workflow: {e}")
        raise HTTPException(status_code=500, detail="Failed to trigger workflow")

@app.get("/api/workflows/{workflow_id}/status")
async def get_workflow_status_endpoint(workflow_id: str):
    """Get the status of a specific workflow."""
    if not workflow_engine:
        raise HTTPException(status_code=503, detail="Workflow engine not initialized")
    
    try:
        status = await workflow_engine.get_workflow_status(workflow_id)
        if "error" in status:
            raise HTTPException(status_code=404, detail=status["error"])
        return status
    except Exception as e:
        logger.error(f"Error getting workflow status for {workflow_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get workflow status")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial data
        initial_reviews = await review_manager.get_pending_reviews()
        await websocket.send_text(json.dumps({"type": "initial_reviews", "data": initial_reviews}, default=str))

        stats = await get_complete_dashboard_stats()
        await websocket.send_text(json.dumps({"type": "dashboard_update", "data": stats.model_dump()}, default=str))

        # Keep connection alive and send periodic updates
        while True:
            await asyncio.sleep(15)  # Send a ping every 15 seconds
            try:
                # You could send a simple ping message
                # await websocket.send_text("ping")

                # Or send updated data periodically
                stats = await get_complete_dashboard_stats()
                await websocket.send_text(json.dumps({"type": "dashboard_update", "data": stats.model_dump()}, default=str))

            except WebSocketDisconnect:
                logger.info("WebSocket disconnected during periodic update.")
                break
            except Exception as e:
                # If the error is about the connection being closed, it's not a server error.
                if "connection closed" in str(e).lower():
                    logger.info(f"WebSocket connection closed pre-emptively: {e}")
                else:
                    # For other errors, we should still log them as errors.
                    logger.error(f"Error sending periodic update: {e}")
                break # Exit loop on other errors too

    except WebSocketDisconnect:
        logger.info("Client disconnected from WebSocket.")
    finally:
        manager.disconnect(websocket)

# Manual trigger for updates
@app.post("/api/trigger-update")
async def trigger_dashboard_update():
    """Manually trigger dashboard update"""
    try:
        stats = await get_complete_dashboard_stats()
        await manager.broadcast({
            "type": "dashboard_stats",
            "data": stats.dict()
        })
        return {
            "message": "Dashboard update triggered", 
            "connections": len(manager.active_connections),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error triggering update: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def watch_collection(collection):
    logger.info(f"Starting watcher for collection: {collection.name}")
    try:
        async with collection.watch(full_document='updateLookup') as stream:
            async for change in stream:
                logger.info(f"Change detected in {collection.name}: {change}")
                await manager.broadcast({
                    "type": "db_update",
                    "collection": collection.name,
                    "data": change
                })
    except Exception as e:
        logger.error(f"Error watching collection {collection.name}: {e}")

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Application startup: connect to DB, init services."""
    global db, rules_engine, workflow_engine, review_manager
    if not await init_database():
        # Exit if DB connection fails
        return

    # Initialize services with direct DB access
    review_manager = SimplifiedHumanReviewManager(db)
    rules_engine = SimplifiedBusinessRulesEngine(db)
    workflow_engine = SimplifiedWorkflowEngine(None, {"workflow_db": db}) # Simplified for this context

    # Set the broadcast function for real-time updates
    review_manager.set_broadcast_function(manager.broadcast)
    
    # Create sample business rules on startup
    await create_sample_rules(db)
    
    # Define and register the invoice processing workflow
    await define_invoice_processing_workflow(workflow_engine, rules_engine, review_manager)

    # Start background tasks to watch collections
    asyncio.create_task(watch_collection(db["invoices"]))
    asyncio.create_task(watch_collection(db["purchase_orders"]))
    asyncio.create_task(watch_collection(db["recent_activity"]))
    logger.info("🚀 Application startup complete.")

@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown: close DB connection."""
    await close_database()
    logger.info("🛑 Application shutdown complete")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
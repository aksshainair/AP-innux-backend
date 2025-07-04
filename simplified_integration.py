# This is a simplified version of human_review_integration.py
# It retains core functionality but removes complex/advanced features for clarity.

from typing import Dict, List, Any, Optional, Union, Callable, TypedDict
from dataclasses import dataclass, field, asdict, is_dataclass
import dataclasses
from enum import Enum
import logging
from datetime import datetime
import uuid
import json

# --- Re-used components from langgraph and motor for context ---
from langgraph.graph import StateGraph, START
# ---

logger = logging.getLogger(__name__)

#region: --- Data Models and Enums ---

class ReviewStatus(Enum):
    PENDING = "pending"
    COMPLETED = "completed"

class ReviewPriority(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3

@dataclass
class ReviewRequest:
    request_id: str
    workflow_id: str
    title: str
    description: str
    data: Dict[str, Any]
    required_decision: List[str]
    priority: ReviewPriority
    assigned_group: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    status: ReviewStatus = ReviewStatus.PENDING

@dataclass
class ReviewResponse:
    request_id: str
    reviewer_id: str
    decision: str
    reasoning: str
    confidence: float
    reviewed_at: datetime = field(default_factory=datetime.now)

class RuleOperator(Enum):
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    GREATER_EQUAL = "greater_equal"
    LESS_EQUAL = "less_equal"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    IN = "in"
    NOT_IN = "not_in"
    REGEX = "regex"
    BETWEEN = "between"

class RuleAction(Enum):
    APPROVE = "approve"
    REJECT = "reject"
    REQUIRE_REVIEW = "require_review"
    NOTIFY = "notify"

@dataclass
class RuleCondition:
    field: str
    operator: RuleOperator
    value: Union[str, int, float, List[Any]]

@dataclass
class RuleConsequence:
    action: RuleAction
    parameters: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 1.0
    message: str = ""

@dataclass
class BusinessRule:
    """Simplified business rule structure"""
    rule_id: str
    name: str
    description: str
    rule_type: str
    priority: int
    conditions: List[RuleCondition]
    consequences: List[RuleConsequence]
    logic_operator: str = "AND"
    active: bool = True
    created_date: datetime = field(default_factory=datetime.now)
    created_by: str = "system"

class RuleEvaluationResult:
    def __init__(self):
        self.fired_rules: List[BusinessRule] = []
        self.final_decision: Optional[RuleAction] = None
        self.confidence: float = 0.0
        self.messages: List[str] = []
        self.modifications: Dict[str, Any] = {}

#endregion

#region: --- Business Rules Engine ---

class SimplifiedBusinessRulesEngine:
    """Simplified business rules engine."""
    
    def __init__(self, db_client):
        self.db = db_client
        
    async def evaluate_rules(self, context: Dict[str, Any], rule_type: str) -> RuleEvaluationResult:
        """Evaluate all applicable rules for given context"""
        try:
            # Fetch rules directly from the database
            rules_data = await self.db.business_rules.find({"rule_type": rule_type, "active": True}).to_list(1000)
            
            rules = []
            for rule_data in rules_data:
                rule = self._convert_to_business_rule(rule_data)
                if rule and rule.active:
                    rules.append(rule)
            
            # Sort by priority (highest first)
            rules.sort(key=lambda r: r.priority, reverse=True)
            
            result = RuleEvaluationResult()
            
            # Evaluate each rule
            for rule in rules:
                if self._evaluate_single_rule(rule, context):
                    result.fired_rules.append(rule)
                    self._apply_rule_consequences(rule, context, result)
                    
                    # NOTE: Early termination logic could be added here if needed
            
            # Resolve conflicts if multiple rules fired
            if len(result.fired_rules) > 1:
                result = self._resolve_rule_conflicts(result, context)
            
            return result
            
        except Exception as e:
            logger.error(f"Error evaluating rules: {e}")
            result = RuleEvaluationResult()
            result.messages.append(f"Error evaluating rules: {str(e)}")
            return result

    def _evaluate_single_rule(self, rule: BusinessRule, context: Dict[str, Any]) -> bool:
        """Evaluate a single rule against context"""
        try:
            condition_results = [self._evaluate_condition(c, context) for c in rule.conditions]
            
            if rule.logic_operator == "AND":
                return all(condition_results)
            elif rule.logic_operator == "OR":
                return any(condition_results)
            return False
        
        except Exception as e:
            logger.error(f"Error evaluating rule {rule.rule_id}: {e}")
            return False
    
    def _evaluate_condition(self, condition: RuleCondition, context: Dict[str, Any]) -> bool:
        """Evaluate a single condition"""
        if condition.field not in context:
            return False
        
        actual_value = context[condition.field]
        expected_value = condition.value
        
        ops = {
            RuleOperator.EQUALS: lambda a, b: a == b,
            RuleOperator.NOT_EQUALS: lambda a, b: a != b,
            RuleOperator.GREATER_THAN: lambda a, b: a > b,
            RuleOperator.LESS_THAN: lambda a, b: a < b,
            RuleOperator.GREATER_EQUAL: lambda a, b: a >= b,
            RuleOperator.LESS_EQUAL: lambda a, b: a <= b,
            RuleOperator.CONTAINS: lambda a, b: b in a,
            RuleOperator.NOT_CONTAINS: lambda a, b: b not in a,
            RuleOperator.IN: lambda a, b: a in b,
            RuleOperator.NOT_IN: lambda a, b: a not in b,
            RuleOperator.REGEX: lambda a, b: bool(__import__("re").search(b, str(a))),
            RuleOperator.BETWEEN: lambda a, b: isinstance(b, list) and len(b) == 2 and b[0] <= a <= b[1]
        }
        
        if condition.operator in ops:
            try:
                return ops[condition.operator](actual_value, expected_value)
            except Exception as e:
                logger.warning(f"Condition error for field '{condition.field}': {e}")
                return False
        return False

    def _apply_rule_consequences(self, rule: BusinessRule, context: Dict[str, Any], result: RuleEvaluationResult):
        """Apply consequences of a fired rule"""
        for consequence in rule.consequences:
            if consequence.message:
                result.messages.append(consequence.message)
            
            if result.final_decision is None: # First rule's decision is taken
                result.final_decision = consequence.action

            if consequence.action == RuleAction.NOTIFY:
                logger.info(f"Notification from rule {rule.rule_id}: {consequence.parameters}")
                
            result.confidence = max(result.confidence, consequence.confidence)
    
    def _resolve_rule_conflicts(self, result: RuleEvaluationResult, context: Dict[str, Any]) -> RuleEvaluationResult:
        """Resolve conflicts: highest priority rule wins"""
        highest_priority_rule = max(result.fired_rules, key=lambda r: r.priority)
        
        new_result = RuleEvaluationResult()
        new_result.fired_rules = result.fired_rules
        self._apply_rule_consequences(highest_priority_rule, context, new_result)
        
        return new_result

    async def create_rule_from_string(self, rule_description: str, context_data: Dict[str, Any], decision: str):
        """
        Creates a simple business rule based on the review context and decision.
        The rule condition is based on the vendor, and the description is provided by the user.
        """
        try:
            # The context for rule evaluation uses 'vendor_name'. The value comes from the review's 'vendor' field.
            vendor_name = context_data.get("vendor")
            if not vendor_name:
                logger.warning(f"Cannot create rule: 'vendor' not found in review context.")
                return

            try:
                # Convert decision like "Approve" to RuleAction.APPROVE
                action = RuleAction(decision.lower())
            except ValueError:
                logger.warning(f"Cannot create rule: invalid decision '{decision}' for rule action.")
                return

            rule_id = f"rule_{uuid.uuid4().hex[:8]}"

            # Construct the document with the correct nested schema
            rule_to_insert = {
                "rule_id": rule_id,
                "rule_type": "discrepancy",
                "created_date": datetime.now(),
                "created_by": "human_review",
                "active": True,
                "usage_count": 0,
                "rule_data": {
                    "name": f"Auto-Rule for vendor: {vendor_name}",
                    "description": rule_description,  # User-provided string
                    "priority": 100,
                    # Condition: IF vendor_name IS 'the_vendor_from_context'
                    "conditions": [{"field": "vendor_name", "operator": RuleOperator.EQUALS.value, "value": vendor_name}],
                    # Consequence: THEN 'the_decision'
                    "consequences": [{"action": action.value, "message": f"Auto-rule from human review: {decision}"}],
                    "logic_operator": "AND"
                }
            }
            
            # Log the generated rule as a JSON string for clarity
            logger.info(f"Generated rule for insertion:\n{json.dumps(rule_to_insert, indent=2, default=str)}")

            await self.db.business_rules.insert_one(rule_to_insert)
            logger.info(f"Successfully created and stored new business rule: {rule_id}")

        except Exception as e:
            logger.error(f"Failed to create rule from description '{rule_description}': {e}", exc_info=True)

    def _convert_to_business_rule(self, rule_data_from_db: Dict[str, Any]) -> Optional[BusinessRule]:
        """Convert a (potentially nested) dictionary from DB to a flat BusinessRule object."""
        try:
            rule_data_from_db.pop('_id', None)
            
            # Unpack the nested rule_data structure into a flat dictionary
            nested_details = rule_data_from_db.pop("rule_data", {})
            flat_rule_data = {**rule_data_from_db, **nested_details}

            # The rest of the function can now process the flattened dictionary
            flat_rule_data["conditions"] = [
                RuleCondition(operator=RuleOperator(c["operator"]), **{k: v for k, v in c.items() if k != 'operator'})
                for c in flat_rule_data.get("conditions", [])
            ]
            flat_rule_data["consequences"] = [
                RuleConsequence(action=RuleAction(c["action"]), **{k: v for k, v in c.items() if k != 'action'})
                for c in flat_rule_data.get("consequences", [])
            ]
            
            if 'created_date' in flat_rule_data and isinstance(flat_rule_data['created_date'], str):
                flat_rule_data['created_date'] = datetime.fromisoformat(flat_rule_data['created_date'])

            valid_keys = {f.name for f in dataclasses.fields(BusinessRule)}
            filtered_data = {k: v for k, v in flat_rule_data.items() if k in valid_keys}

            return BusinessRule(**filtered_data)
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Error converting data to BusinessRule: {e}. Data: {rule_data_from_db}")
            return None

async def create_sample_rules(db_client):
    """Create a set of sample rules for demonstration"""
    rules = [
        BusinessRule(
            rule_id="variance_tolerance_1",
            name="Variance within 5%",
            description="Accepts invoices with total variance up to 5% of PO value.",
            rule_type="variance",
            priority=100,
            conditions=[
                RuleCondition(field="total_variance_percentage", operator=RuleOperator.BETWEEN, value=[0, 5])
            ],
            consequences=[
                RuleConsequence(action=RuleAction.APPROVE, confidence=0.9, message="Approved: Variance within tolerance.")
            ]
        ),
        BusinessRule(
            rule_id="high_value_discrepancy_2",
            name="High-value discrepancy review",
            description="Flags high-value invoices with any discrepancy for urgent review.",
            rule_type="discrepancy",
            priority=200,
            conditions=[
                RuleCondition(field="po_total", operator=RuleOperator.GREATER_THAN, value=10000),
                RuleCondition(field="has_discrepancy", operator=RuleOperator.EQUALS, value=True)
            ],
            consequences=[
                RuleConsequence(action=RuleAction.REQUIRE_REVIEW, parameters={"priority": "URGENT"}, confidence=0.98, message="Urgent review required for high-value PO."),
            ]
        )
    ]

    for rule in rules:
        try:
            rule_dict = asdict(rule)
            for cond in rule_dict['conditions']: cond['operator'] = cond['operator'].value
            for cons in rule_dict['consequences']: cons['action'] = cons['action'].value

            await db_client.business_rules.replace_one(
                {"rule_id": rule.rule_id, "rule_type": rule.rule_type},
                rule_dict,
                upsert=True
            )
            logger.info(f"Stored sample rule: {rule.name}")
        except Exception as e:
            logger.error(f"Error storing sample rule {rule.name}: {e}")

#endregion

#region: --- Workflow Engine ---

class WorkflowStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING_FOR_INPUT = "waiting_for_input"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class WorkflowMetadata:
    workflow_id: str
    workflow_type: str
    status: WorkflowStatus
    created_at: datetime
    updated_at: datetime
    created_by: str = "system"
    priority: int = 0

class SimplifiedWorkflowState(TypedDict, total=False):
    """Simplified workflow state."""
    # Core workflow info
    workflow_id: str
    workflow_type: str
    metadata: WorkflowMetadata
    
    # Invoice processing fields
    invoice_text: str
    po_number: Optional[str]
    extracted_invoice_data: Optional[Dict[str, Any]]
    po_data: Optional[Dict[str, Any]]
    matching_result: Optional[Dict[str, Any]]
    
    # State and decision tracking
    current_node: str
    human_review_required: bool
    final_decision: str
    final_reasoning: str
    errors: List[Dict[str, Any]]

class SimplifiedWorkflowEngine:
    """Manages the lifecycle and execution of simplified workflows."""
    def __init__(self, memory_manager, mongodb_client):
        self.memory_manager = memory_manager
        self.db = mongodb_client["workflow_db"]
        self.workflows_collection = self.db["workflows"]
        self.workflow_definitions = {}

    def register_workflow(self, workflow_type: str, graph: StateGraph):
        self.workflow_definitions[workflow_type] = graph

    async def start_workflow(
        self,
        workflow_type: str,
        initial_state: Dict[str, Any],
        priority: int = 0
    ) -> str:
        """Create and start a new workflow instance"""
        workflow_id = str(uuid.uuid4())
        now = datetime.now()
        
        workflow_metadata = WorkflowMetadata(
            workflow_id=workflow_id,
            workflow_type=workflow_type,
            status=WorkflowStatus.PENDING,
            created_at=now,
            updated_at=now,
            priority=priority,
        )

        current_state = SimplifiedWorkflowState(
            workflow_id=workflow_id,
            workflow_type=workflow_type,
            metadata=workflow_metadata,
            current_node=START,
            errors=[],
            **initial_state
        )
        
        workflow = {
            "workflow_id": workflow_id,
            "workflow_type": workflow_type,
            "status": WorkflowStatus.PENDING.value,
            "current_state": self._serialize_dict(current_state),
        }
        await self.workflows_collection.insert_one(workflow)
        logger.info(f"Started workflow {workflow_id} of type {workflow_type}")
        return workflow_id

    async def execute_workflow(self, workflow_id: str) -> SimplifiedWorkflowState:
        """Execute a workflow from its current state."""
        workflow_doc = await self.workflows_collection.find_one({"workflow_id": workflow_id})
        if not workflow_doc:
            raise ValueError(f"Workflow {workflow_id} not found.")

        graph = self.workflow_definitions.get(workflow_doc["workflow_type"])
        if not graph:
            raise ValueError(f"Workflow type '{workflow_doc['workflow_type']}' not registered")

        app = graph.compile() # No checkpointer for simplicity
        
        await self.workflows_collection.update_one(
            {"workflow_id": workflow_id},
            {"$set": {"status": WorkflowStatus.RUNNING.value}}
        )

        final_state = await app.ainvoke(workflow_doc["current_state"])
        
        await self._save_workflow_state(workflow_id, final_state)
        return final_state

    async def _save_workflow_state(self, workflow_id: str, state: SimplifiedWorkflowState):
        """Update workflow state in the database."""
        update_doc = {
            "$set": {
                "current_state": self._serialize_dict(state),
                "status": state["metadata"]["status"].value,
                "updated_at": datetime.now(),
            }
        }
        await self.workflows_collection.update_one({"workflow_id": workflow_id}, update_doc)

    def _serialize_dict(self, data: Any) -> Any:
        """Recursively serialize a dictionary, converting dataclasses and enums."""
        if is_dataclass(data):
            data = asdict(data)
        if isinstance(data, dict):
            return {key: self._serialize_dict(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._serialize_dict(item) for item in data]
        elif isinstance(data, Enum):
            return data.value
        return data

class SimplifiedWorkflowBuilder:
    """A helper to construct workflow graphs."""
    def __init__(self, workflow_engine: SimplifiedWorkflowEngine):
        self.workflow_engine = workflow_engine
        self.graph = StateGraph(SimplifiedWorkflowState)

    def add_node(self, name: str, handler: Callable):
        self.graph.add_node(name, handler)
        return self
        
    def add_edge(self, from_node: str, to_node: str):
        self.graph.add_edge(from_node, to_node)
        return self
        
    def add_conditional_edge(self, from_node: str, condition_func: Callable, edge_map: Dict[str, str]):
        self.graph.add_conditional_edges(from_node, condition_func, edge_map)
        return self
    
    def build(self, workflow_type: str) -> StateGraph:
        self.workflow_engine.register_workflow(workflow_type, self.graph)
        return self.graph

#endregion

#region: --- Human Review Manager ---

class SimpleNotificationService:
    async def send_notification(self, recipient: str, subject: str, message: str):
        logger.info(f"SIMPLIFIED NOTIFICATION to {recipient}: {subject} - {message}")
        return True

class SimplifiedHumanReviewManager:
    """Simplified human review manager."""
    
    def __init__(self, db_client):
        self.db = db_client
        self.notifications = SimpleNotificationService()
        self.pending_reviews: Dict[str, ReviewRequest] = {}
        self.broadcast_func = None
    
    def set_broadcast_function(self, broadcast_func):
        self.broadcast_func = broadcast_func
    
    async def request_human_review(
        self, 
        workflow_id: str,
        title: str,
        description: str,
        data: Dict[str, Any],
        required_decision: List[str],
        priority: ReviewPriority = ReviewPriority.MEDIUM,
        assigned_group: str = "finance"
    ) -> str:
        """Request human review - simplified version"""
        request_id = str(uuid.uuid4())
        
        review_request = ReviewRequest(
            request_id=request_id,
            workflow_id=workflow_id,
            title=title,
            description=description,
            data=data,
            required_decision=required_decision,
            priority=priority,
            assigned_group=assigned_group,
        )
        
        self.pending_reviews[request_id] = review_request
        await self._store_review_request(review_request)
        await self._send_simple_notification(review_request)
        
        logger.info(f"Created review request {request_id} for workflow {workflow_id}")
        return request_id
        
    async def submit_review_response(
        self, 
        request_id: str, 
        reviewer_id: str, 
        decision: str,
        reasoning: str,
        new_rule: Optional[str] = None
    ) -> bool:
        """Submit a response for a review request"""
        logger.info(f"Submitting review for {request_id} by {reviewer_id}")
        
        # Optionally create a new rule if one was provided.
        # This part is outside the main transaction, as a rule creation failure
        # shouldn't block the review submission itself.
        if new_rule and new_rule.strip():
            try:
                # We fetch the review data first to get context, regardless of status.
                review_request_data = await self.db.review_requests.find_one({"request_id": request_id})
                if review_request_data:
                    context_data = review_request_data.get("data", {})
                    rules_engine = SimplifiedBusinessRulesEngine(self.db)
                    # Pass the review decision to create a context-aware rule
                    await rules_engine.create_rule_from_string(new_rule, context_data, decision)
                else:
                    logger.warning(f"Could not find review request {request_id} for rule creation context.")
            except Exception as e:
                logger.error(f"Non-critical error processing new rule '{new_rule}': {e}", exc_info=True)
        
        # Atomically find and update the review request.
        # This prevents race conditions by ensuring the document is pending when we update it.
        update_result = await self.db.review_requests.update_one(
            {"request_id": request_id, "status": "pending"},
            {
                "$set": {
                    "status": "completed",
                    "response": {
                        "reviewer_id": reviewer_id,
                        "decision": decision,
                        "reasoning": reasoning,
                        "reviewed_at": datetime.now()
                    }
                }
            }
        )
        
        # If no document was modified, it means it wasn't in "pending" state or didn't exist.
        if update_result.modified_count == 0:
            logger.error(f"Failed to update review request {request_id}. It was either not found, not pending, or already updated by another process.")
            return False

        logger.info(f"Review {request_id} completed successfully by {reviewer_id}.")
        
        # Broadcast the completion so the UI can update
        try:
            completed_review = await self.db.review_requests.find_one({"request_id": request_id})
            if completed_review and self.broadcast_func:
                await self.broadcast_func({
                    "type": "completed_review",
                    "data": self._convert_review_for_broadcast(completed_review)
                })
        except Exception as e:
            logger.error(f"Failed to broadcast review completion for {request_id}: {e}")
            
        return True

    async def get_pending_reviews(self) -> List[Dict[str, Any]]:
        """Get all pending human review requests"""
        try:
            db_reviews = await self.db.review_requests.find(
                {"status": "pending"}
            ).sort("created_at", -1).to_list(100)
            
            return [self._convert_review_for_broadcast(r) for r in db_reviews]
        except Exception as e:
            logger.error(f"Error getting pending reviews: {e}")
            return []
        
    def _convert_review_for_broadcast(self, review_data: Dict) -> Dict[str, Any]:
        """Convert a review from DB to a dictionary suitable for broadcasting"""
        if review_data.get('created_at') and isinstance(review_data['created_at'], datetime):
            review_data['created_at'] = review_data['created_at'].isoformat()
        review_data.pop('_id', None)
        return review_data

    async def _send_simple_notification(self, review_request: ReviewRequest):
        if self.broadcast_func:
            await self.broadcast_func({
                "type": "new_review",
                "data": self._convert_review_for_broadcast(asdict(review_request))
            })

    async def _store_review_request(self, review_request: ReviewRequest):
        """Stores the review request document in MongoDB."""
        review_dict = asdict(review_request)
        # Convert enums to their string values for JSON/BSON compatibility
        review_dict['status'] = review_dict['status'].value
        review_dict['priority'] = review_dict['priority'].value
        
        await self.db.review_requests.insert_one(review_dict)
        logger.info(f"Stored human review request {review_request.request_id} in the database.")

    async def _store_review_response(self, response: ReviewResponse):
        """Stores the response to a review in the database."""
        await self.db.review_responses.insert_one(asdict(response))
        await self.db.review_requests.update_one(
            {"request_id": response.request_id},
            {"$set": {"status": ReviewStatus.COMPLETED.value}}
        )

#endregion 
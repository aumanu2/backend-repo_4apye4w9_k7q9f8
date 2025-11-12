"""
Database Schemas

Define your MongoDB collection schemas here using Pydantic models.
These schemas are used for data validation in your application.

Each Pydantic model represents a collection in your database.
Model name is converted to lowercase for the collection name:
- User -> "user" collection
- Product -> "product" collection
- BlogPost -> "blogs" collection
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

# Example schemas (kept for reference):

class User(BaseModel):
    """
    Users collection schema
    Collection name: "user" (lowercase of class name)
    """
    name: str = Field(..., description="Full name")
    email: str = Field(..., description="Email address")
    address: str = Field(..., description="Address")
    age: Optional[int] = Field(None, ge=0, le=120, description="Age in years")
    is_active: bool = Field(True, description="Whether user is active")

class Product(BaseModel):
    """
    Products collection schema
    Collection name: "product" (lowercase of class name)
    """
    title: str = Field(..., description="Product title")
    description: Optional[str] = Field(None, description="Product description")
    price: float = Field(..., ge=0, description="Price in dollars")
    category: str = Field(..., description="Product category")
    in_stock: bool = Field(True, description="Whether product is in stock")

# App-specific schemas

class Dataset(BaseModel):
    """
    Datasets uploaded by users
    Collection name: "dataset"
    """
    name: str = Field(..., description="Dataset name provided by user")
    columns: List[str] = Field(..., description="Column names in the dataset")
    column_types: Dict[str, str] = Field(..., description="Inferred type per column: string, number, boolean, date")
    row_count: int = Field(0, ge=0, description="Total number of rows stored")

class Record(BaseModel):
    """
    Individual rows for a dataset
    Collection name: "record"
    """
    dataset_id: str = Field(..., description="Reference to Dataset _id as string")
    data: Dict[str, Any] = Field(..., description="Raw row data as key-value pairs")

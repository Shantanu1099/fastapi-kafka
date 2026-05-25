from pydantic import BaseModel, Field


class ProduceMessage(BaseModel):
    # defining the schema for the message to be produced (request fields)
    message: str = Field(
        min_length=1, max_length=255, description="The message to be produced"
    )

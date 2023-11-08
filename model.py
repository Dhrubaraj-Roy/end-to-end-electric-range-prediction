from pydantic import BaseModel

#Class which describes Bank Notes measurements
class mymodel(BaseModel):
    Postal_Code: int
    Model_Year: int
    Legislative_District: int
    
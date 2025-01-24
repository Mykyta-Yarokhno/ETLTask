### Project Details

- **Number of rows in table after running the program are**: 29,889  
- **SQL scripts used for creating the database and tables**: ETLTask/Scripts/CreateDb.sql
- **Сomments on my assumptions made**:
  - Instead of loading the entire file into memory, we can process it in small chunks. This prevents memory exhaustion and keeps resource usage manageable.
  - We can process different parts of the file in parallel to take advantage of multi-core processors (using PLINQ). 

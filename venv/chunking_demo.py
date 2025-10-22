import pandas as pd
from sqlalchemy import text
from db_connect import engine


# Empty list to store partial results
chunks= []


# Step 2: Run query in streaming mode (does not load all at once)
with engine.connect() as conn:
    print("Connection Eastablished , reading data in chunks..")
    
    #Query with real table
    query = text("Select * from dbo.Hospital_Management")

    # Stream data row by row
    rs = conn.execution_options(stream_results=True).execute(query)

    # Step 3: Read 10,000 rows at a time
    while True:
        chunk = rs.fetchmany(1000)
        if not chunk:
            break

        # Convert chunk → DataFrame
        df_chunk = pd.DataFrame(chunk, columns=rs.keys())
        
        # Step 4: Process each chunk (Example: Revenue by Department)
        grouped = df_chunk.groupby('Department')['AmountPaid'].sum()
        chunks.append(grouped)

        print(f"✅ Processed chunk with {len(df_chunk)} rows")


# Step 5: Combine all chunk results
final_df = pd.concat(chunks).groupby(level=0).sum().reset_index()

print("\n🏁 Final Aggregated Revenue by Department:")
print(final_df)

# Optional: Save result to CSV
final_df.to_csv("data/department_revenue_chunked.csv", index=False)
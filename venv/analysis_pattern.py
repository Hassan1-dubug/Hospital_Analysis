import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from db_connect import engine

# Step 01: Load data
df = pd.read_sql_query("SELECT * FROM dbo.Hospital_Management", engine)
print("✅ Data Loaded:", df.shape)

# Convert dates (if AdmissionDate exists)
df['AdmissionDate'] = pd.to_datetime(df['AdmissionDate'])

# Step 02: Monthly Revenue Trend
monthly_revenue = (
    df.groupby(pd.Grouper(key='AdmissionDate', freq='M'))['AmountPaid']
    .sum()
    .reset_index()
)
plt.figure(figsize=(10, 6))
plt.plot(monthly_revenue['AdmissionDate'], monthly_revenue['AmountPaid'], marker='o')
plt.title('📈 Monthly Revenue Trend')
plt.xlabel('Month')
plt.ylabel('Total Revenue')
plt.grid(True)
plt.show()

# Step 03: Top 5 Doctors by Revenue
top_doctors = (
    df.groupby('DoctorName')['AmountPaid']
    .sum()
    .reset_index()
    .sort_values('AmountPaid', ascending=False)
    .head(5)
)
plt.figure(figsize=(8, 5))
sns.barplot(x='DoctorName', y='AmountPaid', data=top_doctors, palette='viridis')
plt.title('🏆 Top 5 Doctors by Revenue')
plt.xticks(rotation=45)
plt.show()

# Step 04: Department-Wise Average Feedback
dept_feedback = (
    df.groupby('Department')['FeedbackRating']
    .mean()
    .reset_index()
    .sort_values('FeedbackRating', ascending=False)
)
plt.figure(figsize=(8, 5))
sns.barplot(x='Department', y='FeedbackRating', data=dept_feedback, palette='coolwarm')
plt.title('⭐ Average Feedback by Department')
plt.xticks(rotation=45)
plt.show()

# Step 05: Anomaly Detection (Outlier Amounts)
Q1 = df['AmountPaid'].quantile(0.25)
Q3 = df['AmountPaid'].quantile(0.75)
IQR = Q3 - Q1
upper_limit = Q3 + 1.5 * IQR
outliers = df[df['AmountPaid'] > upper_limit]

print(f"⚠️ Found {len(outliers)} high-value transactions (possible anomalies)")
plt.figure(figsize=(8, 5))
sns.boxplot(x=df['AmountPaid'])
plt.title('💰 Outlier Detection (Amount Paid)')
plt.show()

# Step 06: Returning vs New Patients (Churn Analysis)
patient_visits = df.groupby('PatientID')['AdmissionDate'].nunique().reset_index()
patient_visits['Type'] = patient_visits['AdmissionDate'].apply(
    lambda x: 'Returning' if x > 1 else 'New'
)
plt.figure(figsize=(6, 4))
sns.countplot(x='Type', data=patient_visits, palette='Set2')
plt.title('🔁 Returning vs New Patients')
plt.show()

# Step 07: Save Results Back to SQL (optional)
monthly_revenue.to_sql('Hospital_Monthly_Revenue', engine, schema='dbo', if_exists='replace', index=False)
top_doctors.to_sql('Hospital_Top_Doctors', engine, schema='dbo', if_exists='replace', index=False)
dept_feedback.to_sql('Hospital_Dept_Feedback_Summary', engine, schema='dbo', if_exists='replace', index=False)
outliers.to_sql('Hospital_Anomalies', engine, schema='dbo', if_exists='replace', index=False)
patient_visits.to_sql('Hospital_Patient_Visit_Types', engine, schema='dbo', if_exists='replace', index=False)

print("\n🏁 All Advanced Analysis Tables saved to SQL Server successfully!")

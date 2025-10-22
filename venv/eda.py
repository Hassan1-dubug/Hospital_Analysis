import pandas as pd
import seaborn as sb
import matplotlib.pyplot as plt
from db_connect import engine

# step: 01 load data from SQL Server
df = pd.read_sql_query("Select top 10000 * from dbo.Hospital_Management" , engine)

# step:02 Basic info
print("shape(row,column):", df.shape)
print("\n Data Type: \n" , df.dtypes)
print("\n Summary Statistics: \n" , df.describe(include="all"))

# step: 03 Checking missing value 
print("\n Missing value \n" , df.isna().sum().sort_values(ascending=False).head(20))

# Step 4: Key Questions (EDA Queries)
# Total Patients
total_patients = df['PatientID'].nunique()
print("\n Total Patients \n" , total_patients)

# Patients in each department
patients_by_dept = df['Department'].value_counts()
print("\n Patients per Dept \n" , patients_by_dept)

#  Total Amount (Billing)
total_billing = df['TotalBillAmount'].sum()
print("\n Total Bill Amount \n" , total_billing)

#  Average Age of Patients
avg_age = df['Age'].mean()
print("\n Avg Age of Patient \n" , round(avg_age,2))

#  Total Doctor 
total_doctor = df['DoctorID'].nunique()
print("\n Total Doctor \n" , total_doctor)

#  Total Amount Paid
print("\n Total Amount Paid \n" , df['AmountPaid'].sum())

#  Total Revenue by Doctor
rev_by_doctor = df.groupby('DoctorName')['AmountPaid'].sum().reset_index().sort_values('AmountPaid' , ascending=False)
print("\n Revenue by Doctor \n" , rev_by_doctor.head(10))

#  Department-wise Revenue
dept_rev = df.groupby('Department')['AmountPaid'].sum().reset_index().sort_values('AmountPaid' , ascending=False)
print("\n Department wise Revenue \n" , dept_rev)

#  Average Feedback by Department
avg_feed = df.groupby('Department')['FeedbackRating'].mean().reset_index().sort_values('FeedbackRating', ascending=False)
print("\n 9 Avg Feedback by Depart \n " , avg_feed)

#  Total Payment by Payment Mode
payment_by_mode = df.groupby('PaymentMode')['AmountPaid'].sum().reset_index().sort_values('AmountPaid', ascending=False)
print("\nTotal Payment by Mode:\n", payment_by_mode)

# Step 5: Simple Visualization Examples

plt.figure(figsize=(10,6))
df['Department'].value_counts().plot(kind='bar')
plt.title("Patients per Department")
plt.xlabel("Department")
plt.ylabel("Patient Count")
plt.show()

plt.figure(figsize=(10,6))
sb.barplot(x='Department', y='AmountPaid', data=df, estimator='sum', ci=None)
plt.title("Revenue by Department")
plt.xticks(rotation=45)
plt.show()


# ✅ Summary KPIs (Total Patients, Doctors, Billing, etc.)
summary_data = {
    'Metric': [
        'Total Patients',
        'Total Doctors',
        'Total Billing',
        'Total Amount Paid',
        'Average Age'
    ],
    'Value': [
        total_patients,
        total_doctor,
        total_billing,
        df['AmountPaid'].sum(),
        round(avg_age, 2)
    ]
}

summary_df = pd.DataFrame(summary_data)

# ✅ 01 Write KPI Summary
summary_df.to_sql(
    'Hospital_KPI_Summary',
    engine,
    schema='dbo',
    if_exists='replace',
    index=False
)
print("\n✅ KPI Summary Table written to SQL Server (dbo.Hospital_KPI_Summary)")

# ✅ 02 Write Department-wise Revenue
dept_rev.to_sql(
    'Hospital_Dept_Revenue',
    engine,
    schema='dbo',
    if_exists='replace',
    index=False
)
print("✅ Department Revenue written to SQL Server (dbo.Hospital_Dept_Revenue)")

# ✅ 03 Write Doctor-wise Revenue
rev_by_doctor.to_sql(
    'Hospital_Doctor_Revenue',
    engine,
    schema='dbo',
    if_exists='replace',
    index=False
)
print("✅ Doctor Revenue written to SQL Server (dbo.Hospital_Doctor_Revenue)")

# ✅ 04 Write Average Feedback by Department
avg_feed.to_sql(
    'Hospital_Dept_Feedback',
    engine,
    schema='dbo',
    if_exists='replace',
    index=False
)
print("✅ Department Feedback written to SQL Server (dbo.Hospital_Dept_Feedback)")

# ✅ 05 Write Payment Mode Summary
payment_by_mode.to_sql(
    'Hospital_Payment_Summary',
    engine,
    schema='dbo',
    if_exists='replace',
    index=False
)
print("✅ Payment Summary written to SQL Server (dbo.Hospital_Payment_Summary)")

print("\n🏁 All analysis results successfully saved to SQL Server!")

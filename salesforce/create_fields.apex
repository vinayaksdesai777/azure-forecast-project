List<Metadata.CustomField> fields = new List<Metadata.CustomField>();
String[] textFields = new String[]{'Product_Id__c','Location_Id__c','Customer_Id__c','Channel__c','Category__c','Sub_Category__c','Region__c','Country__c','Currency_Code__c','UOM__c','Period_Type__c','Fiscal_Period__c'};
for(String fn : textFields){
  Metadata.CustomField f = new Metadata.CustomField();
  f.fullName = 'Forecast__c.' + fn;
  f.label = fn.replace('__c','').replace('_',' ');
  f.type_x = Metadata.FieldType.Text;
  f.length = 100;
  fields.add(f);
}
Metadata.CustomField d = new Metadata.CustomField();
d.fullName='Forecast__c.Forecast_Date__c'; d.label='Forecast Date'; d.type_x=Metadata.FieldType.Date;
fields.add(d);
Metadata.CustomField q = new Metadata.CustomField();
q.fullName='Forecast__c.Forecast_Qty__c'; q.label='Forecast Qty'; q.type_x=Metadata.FieldType.Number; q.precision=16; q.scale=2;
fields.add(q);
Metadata.CustomField r = new Metadata.CustomField();
r.fullName='Forecast__c.Revenue_Amount__c'; r.label='Revenue Amount'; r.type_x=Metadata.FieldType.Number; r.precision=18; r.scale=2;
fields.add(r);
Metadata.DeployContainer c = new Metadata.DeployContainer();
for(Metadata.CustomField f : fields){ c.addMetadata(f); }
Metadata.Operations.enqueueDeployment(c, null);
System.debug('Queued: ' + fields.size());
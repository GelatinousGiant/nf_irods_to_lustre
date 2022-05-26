#!/usr/bin/env python
# SELECT DISTINCT sample.name as sanger_sample_id,original_study.name as cohort,iseq_flowcell.id_study_tmp, study.id_study_lims, study.last_updated, sample.public_name, sample.donor_id, iseq_run_lane_metrics.instrument_model, 	iseq_run_lane_metrics.instrument_external_name, iseq_run_lane_metrics.instrument_name,
# 	donors.supplier_name as donor FROM mlwarehouse.iseq_flowcell
# JOIN mlwarehouse.sample ON iseq_flowcell.id_sample_tmp = sample.id_sample_tmp
# JOIN mlwarehouse.study ON iseq_flowcell.id_study_tmp = study.id_study_tmp
# JOIN mlwarehouse.iseq_product_metrics ON iseq_flowcell.id_iseq_flowcell_tmp = iseq_product_metrics.id_iseq_flowcell_tmp 
# JOIN mlwarehouse.iseq_run on iseq_run.id_run = iseq_product_metrics.id_run 
# JOIN mlwarehouse.iseq_run_lane_metrics on iseq_run_lane_metrics.id_run = iseq_run.id_run
# JOIN mlwarehouse.psd_sample_compounds_components pscc ON iseq_flowcell.id_sample_tmp = pscc.compound_id_sample_tmp
# JOIN mlwarehouse.sample as donors ON donors.id_sample_tmp = pscc.component_id_sample_tmp
# JOIN mlwarehouse.stock_resource ON donors.id_sample_tmp = stock_resource.id_sample_tmp
# JOIN mlwarehouse.study as original_study ON original_study.id_study_tmp = stock_resource.id_study_tmp
# WHERE sample.name IN ('CRD_CMB12813653','CRD_CMB12813652','CRD_CMB12813646','CRD_CMB12813650','CRD_CMB12813655','CRD_CMB12813654','CRD_CMB12813649','CRD_CMB12813647','CRD_CMB12813648','CRD_CMB12813651');



__date__ = '2022-04-01'
__version__ = '0.0.1'

import argparse
import pandas as pd
import mysql.connector

try:
    import imp
    mod = imp.load_source("module_name", '/lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/secret/mlwpw.py')
    PASSWORD = mod.PASSWORD
    USERNAME = mod.USERNAME
    PORT = mod.PORT
    HOST = mod.HOST
    DATABASE = mod.DATABASE
except:
    PASSWORD = ''
    USERNAME = ''   
    PORT = ''
    HOST = ''
    DATABASE = ''



def get_samples_reception_metadata(Reviewed_metadata_donors):
    try:
        # Here we connect to Titian database from samples reception and extract the required fields - such as volume or any other issues with samples.
        import imp
        import cx_Oracle
        samples = list(set(Reviewed_metadata_donors.donor.values))
        mod = imp.load_source("module_name", '/lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/secret/titan.py')
        T_PASSWORD = mod.PW
        T_USER = mod.USERNAME
        T_DSN = mod.DNS2
        Search_IDs = '\''+'\',\''.join(set(samples))+'\''
        connection = cx_Oracle.connect(user=T_USER, password=T_PASSWORD,dsn=T_DSN)
        cursor = connection.cursor()
        cursor.execute(f'SELECT r.* FROM mosaic.container_report r WHERE r."Barcode" IN ({Search_IDs})')
        Reviewed_metadata_donors_barcodes = pd.DataFrame(cursor.fetchall())
        if not Reviewed_metadata_donors_barcodes.empty:
            field_names = [i[0] for i in cursor.description]
            Reviewed_metadata_donors_barcodes.columns = field_names
        Reviewed_metadata_donors_barcodes = Reviewed_metadata_donors_barcodes.set_index('Barcode')
        Reviewed_metadata_donors_barcodes[['Issue','Creation date','Amount']]
        # here we now add the extra metadata to the Reviewed_metadata_donors
        Reviewed_metadata_donors = Reviewed_metadata_donors.reset_index().set_index('donor')
        Reviewed_metadata_donors[['Issue','Date Sample Recieved','Amount']]=Reviewed_metadata_donors_barcodes[['Issue','Creation date','Amount']]
        Reviewed_metadata_donors = Reviewed_metadata_donors.reset_index().set_index('name')
        # Reviewed_metadata_donors.to_csv('/lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/fetch/Fetch_May9_2022/nf_irods_to_lustre/test.csv')
        return Reviewed_metadata_donors
    except:
        return Reviewed_metadata_donors

def main():
    """Run CLI."""
    parser = argparse.ArgumentParser(
        description="""
            Produces an input file for the yascp pipeline, by taking the sanger_sample_ids from the raw.file_paths_10x.tsv
            """
    )
    parser.add_argument(
        '-v', '--version',
        action='version',
        version='%(prog)s {version}'.format(version=__version__)
    )
    parser.add_argument(
         '-f10x', '--file_paths_10x',
        action='store',
        dest='file_paths_10x',
        required=True,
        help='Path to 10x data.'
    )

    options = parser.parse_args()
    # Get clean names
    file_paths_10x = options.file_paths_10x
    Data = pd.read_csv(file_paths_10x,sep='\t')
    Search_IDs = '\''+'\',\''.join(set(Data.experiment_id))+'\''
    Data['n_pooled']='1' #add these if availabe in mlwh
    Data['donor_vcf_ids']='none1' #add these if availabe in mlwh
    Data['data_path_10x_format']=Data['data_path_10x_format']+'/..'
    # do the mysql fetch for extra metadata.
    # if available add the number of samples pooled and produce the correct input format.
    if (USERNAME!=''):
        # if we have successfuly sourced the credentials for the mlwh then get all the extra metadata.
        mydb = mysql.connector.connect(
            host=HOST,port=PORT,
            user=USERNAME,
            passwd=PASSWORD,
            database=DATABASE,
            auth_plugin='mysql_native_password'
        )
        mycursor = mydb.cursor()
        sql = f"SELECT sample1.name as experiment_id,sample1.cohort,sample1.id_study_tmp, sample1.id_study_lims, sample1.last_updated, sample1.public_name, sample1.donor_id, sample1.instrument_model, 	sample1.instrument_external_name, sample1.instrument_name, \
                COUNT(sample1.name) as n_pooled FROM (SELECT DISTINCT sample.name,original_study.name as cohort,iseq_flowcell.id_study_tmp, study.id_study_lims, study.last_updated, sample.public_name, sample.donor_id, iseq_run_lane_metrics.instrument_model, 	iseq_run_lane_metrics.instrument_external_name, iseq_run_lane_metrics.instrument_name, \
                donors.supplier_name as donor FROM mlwarehouse.iseq_flowcell \
                    JOIN mlwarehouse.sample ON iseq_flowcell.id_sample_tmp = sample.id_sample_tmp \
                    JOIN mlwarehouse.study ON iseq_flowcell.id_study_tmp = study.id_study_tmp \
                    JOIN mlwarehouse.iseq_product_metrics ON iseq_flowcell.id_iseq_flowcell_tmp = iseq_product_metrics.id_iseq_flowcell_tmp \
                    JOIN mlwarehouse.iseq_run on iseq_run.id_run = iseq_product_metrics.id_run \
                    JOIN mlwarehouse.iseq_run_lane_metrics on iseq_run_lane_metrics.id_run = iseq_run.id_run \
                    JOIN mlwarehouse.psd_sample_compounds_components pscc ON iseq_flowcell.id_sample_tmp = pscc.compound_id_sample_tmp \
                    JOIN mlwarehouse.sample as donors ON donors.id_sample_tmp = pscc.component_id_sample_tmp \
                    JOIN mlwarehouse.stock_resource ON donors.id_sample_tmp = stock_resource.id_sample_tmp \
                    JOIN mlwarehouse.study as original_study ON original_study.id_study_tmp = stock_resource.id_study_tmp \
                    WHERE sample.name IN ({Search_IDs})) AS sample1 WHERE 1 GROUP BY name;"
        mycursor.execute(sql)
        Reviewed_metadata = pd.DataFrame(mycursor.fetchall())
        if not Reviewed_metadata.empty:
            field_names = [i[0] for i in mycursor.description]
            Reviewed_metadata.columns = field_names
        if len(Reviewed_metadata)==0:
            sql = f"SELECT DISTINCT sample.sanger_sample_id as experiment_id,sample.cohort, iseq_flowcell.id_study_tmp, study.id_study_lims, study.last_updated, sample.public_name, sample.donor_id, iseq_run_lane_metrics.instrument_model, iseq_run_lane_metrics.instrument_external_name, iseq_run_lane_metrics.instrument_name \
                FROM mlwarehouse.iseq_flowcell \
                JOIN mlwarehouse.sample ON iseq_flowcell.id_sample_tmp = sample.id_sample_tmp \
                JOIN mlwarehouse.study ON iseq_flowcell.id_study_tmp = study.id_study_tmp \
                JOIN mlwarehouse.iseq_product_metrics ON iseq_flowcell.id_iseq_flowcell_tmp = iseq_product_metrics.id_iseq_flowcell_tmp \
                JOIN mlwarehouse.iseq_run on iseq_run.id_run = iseq_product_metrics.id_run \
                JOIN mlwarehouse.iseq_run_lane_metrics on iseq_run_lane_metrics.id_run = iseq_run.id_run \
                WHERE sample.sanger_sample_id IN ({Search_IDs});"
            mycursor.execute(sql)
            Reviewed_metadata = pd.DataFrame(mycursor.fetchall())
            if not Reviewed_metadata.empty:
                field_names = [i[0] for i in mycursor.description]
                Reviewed_metadata.columns = field_names

        Reviewed_metadata['instrument'] = Reviewed_metadata['instrument_model']+'_'+Reviewed_metadata['instrument_name']+'_'+Reviewed_metadata['instrument_external_name']
        sql_donors = f"SELECT DISTINCT sample.name as experiment_id,original_study.name as cohort,donors.customer_measured_volume, \
                        donors.supplier_name as donor FROM mlwarehouse.iseq_flowcell \
                        JOIN mlwarehouse.sample ON iseq_flowcell.id_sample_tmp = sample.id_sample_tmp \
                        JOIN mlwarehouse.study ON iseq_flowcell.id_study_tmp = study.id_study_tmp \
                        JOIN mlwarehouse.iseq_product_metrics ON iseq_flowcell.id_iseq_flowcell_tmp = iseq_product_metrics.id_iseq_flowcell_tmp \
                        JOIN mlwarehouse.iseq_run on iseq_run.id_run = iseq_product_metrics.id_run \
                        JOIN mlwarehouse.iseq_run_lane_metrics on iseq_run_lane_metrics.id_run = iseq_run.id_run \
                        JOIN mlwarehouse.psd_sample_compounds_components pscc ON iseq_flowcell.id_sample_tmp = pscc.compound_id_sample_tmp \
                        JOIN mlwarehouse.sample as donors ON donors.id_sample_tmp = pscc.component_id_sample_tmp \
                        JOIN mlwarehouse.stock_resource ON donors.id_sample_tmp = stock_resource.id_sample_tmp \
                        JOIN mlwarehouse.study as original_study ON original_study.id_study_tmp = stock_resource.id_study_tmp \
                        WHERE sample.name IN ({Search_IDs})" 
                        # 'Barcode' IN ('S2-046-00331','S2-046-00691')

        # Here we can also establish the cohorts and generate extra metadata per sample. 
        # Have to also retrieve metadata from titin sample reception.

        mycursor.execute(sql_donors)
        Reviewed_metadata_donors = pd.DataFrame(mycursor.fetchall())
        if not Reviewed_metadata_donors.empty:
            field_names = [i[0] for i in mycursor.description]
            Reviewed_metadata_donors.columns = field_names


        Data = Data.set_index('experiment_id')
        Reviewed_metadata=Reviewed_metadata.set_index('experiment_id')
        Reviewed_metadata_donors = Reviewed_metadata_donors.set_index('experiment_id')
        try:
            Data['n_pooled']=Reviewed_metadata['n_pooled']
            for idx1 in Data.index:
                print(idx1)
                dons = ','.join(list(Reviewed_metadata_donors.loc[idx1]['donor'].values))
                Data.loc[idx1,'donor_vcf_ids']='\''+dons+'\''
        except:
            Data['n_pooled']=1

        Data=Data.reset_index()
        Reviewed_metadata.reset_index(inplace=True)
       
        Reviewed_metadata.to_csv('Extra_Metadata.tsv',sep='\t', index=False)
        try:
            # if len(Reviewed_metadata_donors)!=0:
            #     Reviewed_metadata_donors=Reviewed_metadata_donors.set_index('experiment_id')
            Reviewed_metadata = Reviewed_metadata.set_index('experiment_id')

            Reviewed_metadata_donors = get_samples_reception_metadata(Reviewed_metadata_donors)
            Reviewed_metadata_donors.reset_index(inplace=True)
            Reviewed_metadata_donors = Reviewed_metadata_donors.set_index('experiment_id')
            for col1 in Reviewed_metadata.columns:
                Reviewed_metadata_donors[col1]=Reviewed_metadata[col1]
            Reviewed_metadata_donors=Reviewed_metadata_donors.reset_index()
            Reviewed_metadata_donors['experiment_id']=Reviewed_metadata_donors['experiment_id']+'__'+Reviewed_metadata_donors['donor']
            Reviewed_metadata_donors=Reviewed_metadata_donors.set_index('experiment_id')
            Reviewed_metadata_donors.to_csv('Extra_Metadata_Donors.tsv',sep='\t')

        except:
            print("donor_metadata not available")
        # Here we also want to querry the google sheet to retrieve additional metrics - Date sample recieved, Low viability, Low volume on arrival.
        
    # Data2 = Data.loc['experiment_id','n_pooled','donor_vcf_ids','data_path_10x_format']
    Data2 = Data.loc[:,['experiment_id','n_pooled','donor_vcf_ids','data_path_10x_format']]
    Data2.to_csv('input.tsv',sep='\t', index=False)
    

if __name__ == '__main__':
    main()
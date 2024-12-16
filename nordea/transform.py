import pandas as pd

class Transformer:

    """Transformer class is a generic class used to apply commom transformation tasks over a dataframe
    """
  
    def __init__(self,data_path='data'):
        """_summary_

        Args:
            data_path (str, optional): Root path where data files are saved. Defaults to 'data'.
        """
        self.data_path = data_path
        self.df = None


    def load(self,file_name : str) -> object:
        """_summary_

        Args:
            file_name (str): Load csv file from data_path using pandas

        Returns:
            Transformer: returns the current instance of transformer class
        """
        self.df = pd.read_csv(f"{self.data_path}/{file_name}")
        return self
    
    def unique_rows(self) -> object:
        """Drop duplicates in the dataframe

        Returns:
            Transformer: returns the current instance of transformer class
        """
        self.df.drop_duplicates(inplace=True)
        return self
    
    def filter_df(self,criteria : str) -> object:
        """_summary_

        Args:
            criteria (str): Filter criteria to be used in the query method of a pandas dataframe.

        Returns:
            Transformer: returns the current instance of transformer class
        """
        self.df.query(criteria,inplace=True)
        return self
    
    def rename_columns(self,columns_renamed_dict : dict) -> object:
        """Rename columns in the dataframe

        Args:
            columns_renamed_dict (dict): Dictionary containing the column names (as keys) that will be renamed and the new names (values)

        Returns:
            Transformer: returns the current instance of transformer class
        """
        self.df.rename(columns=columns_renamed_dict, inplace=True)
        return self
    
    def select_columns(self,columns : list) -> object:
        """Select specific columns of a dataframe

        Args:
            columns (_type_): List of columns to be selected

        Returns:
            Transformer: returns the current instance of transformer class
        """
        self.df = self.df[columns]
        return self
    
    def custom(self,column : str,lambda_function : object) -> object:
        """Apply custom transformation on a column in the dataframe

        Args:
            column (str): Name of the column
            lambda_function (object): Lambda function that will be used to apply the transformations

        Returns:
            Transformer: returns the current instance of transformer class
        """
        self.df[column] = self.df.apply(lambda_function,axis=1)
        return self
    
    def sum(self,group_columns : list,aggregation_columns : list) -> object:
        """_summary_

        Args:
            group_columns (list): List of columns that will be in the group by clause
            aggregation_columns (list): List of columns that will summed

        Returns:
            Transformer: returns the current instance of transformer class
        """
        self.df = self.df.groupby(group_columns)[aggregation_columns].sum().reset_index()
        return self
    def save(self,output_file_name,format='csv',index=False):
        if format == 'jsonl':
            self.df.to_json(f"{self.data_path}/{output_file_name}",orient='records', lines=True,index=index)
        elif format == 'csv':
            self.df.to_csv(f"{self.data_path}/{output_file_name}",index=index)
        else:
            raise Exception('Format not implemented')
    
    def join_datasets(self,left_path : str, right_path : str,key : str) -> object:
        """Join two dataframes (inner)

        Args:
            left_path (str): Path of the first dataset
            right_path (str): Path of the second dataset
            key (str): Key that will be used in the join

        Returns:
            Transformer: returns the current instance of transformer class
        """
        left_df = pd.read_csv(f"{self.data_path}/{left_path}").set_index(key)
        right_df = pd.read_csv(f"{self.data_path}/{right_path}").set_index(key)
        left_df = left_df.join(right_df, on=key)
        self.df = left_df.reset_index()
        return self
    
    def drop_columns(self, columns : list) -> object:
        """Drop columns

        Args:
            columns (list): List of columns to be dropped

        Returns:
            Transformer: Instance of transformer class
        """
        self.df = self.df.drop(columns,axis=1)
        return self

    def add_incremental_id(self,name):
        """Add a new incremental id column to the dataframe

        Args:
            name (str): Name of the new column

        Returns:
            Transformer: Instance of transformer class
        """
        self.df.insert(0, name, range(1, 1 + len(self.df)))
        return self

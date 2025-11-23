from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class StringCleaner:
    """
    Utility class to clean string columns such as names, addresses, 
    and phone numbers.
    """
   

    @staticmethod
    def clean_string_column(df: DataFrame, column_name: str) -> DataFrame:
        """
        Cleans a string column by:
        - Removing special characters
        - Collapsing multiple spaces
        - Trimming whitespace
        - Converting to Title Case
        """

        return (
        df.withColumn(
            column_name,
            F.initcap(
                F.trim(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.col(column_name),
                            r"[^A-Za-z ]",     
                            " "
                        ),
                        r"\s+",
                        " "
                    )
                )
            )
        )
    )

    @staticmethod
    def clean_phone_column(df: DataFrame, column_name: str) -> DataFrame:
        """
        Safely clean phone numbers:
        - Extract digits
        - Validate length (>= 7)
        - Extract extension
        - Return NULL for invalid values
        - Retain original formatting where valid
        """

        col = F.col(column_name)

        # 1. NULL out obvious errors
        df = df.withColumn(
            column_name,
            F.when(col.rlike("#ERROR!"), None)
             .when(F.trim(col) == "", None)
             .otherwise(col)
        )

        # 2. Extract digits only
        digits = F.regexp_replace(col, r"[^0-9]", "")

        # 3. Extract extension
        ext = F.regexp_extract(col, r"[xX](\d+)", 1)

        # 4. Validate length ≥ 7
        valid_number = F.when(F.length(digits) >= 7, col).otherwise(None)

        # 5. Add extension if valid
        final_val = F.when(
            valid_number.isNotNull() & (ext != ""),
            F.concat(valid_number, F.lit(" x"), ext)
        ).otherwise(valid_number)

        return df.withColumn(column_name, final_val)

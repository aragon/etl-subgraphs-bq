import pandas as pd
import ast



class Spaces:
    def __init__(
        self,
        df : pd.DataFrame):

        self.df = df

    def _get_spaces_strategies(self):
        fields = []
        self.df.strategies = self.df.strategies.apply(lambda x: ast.literal_eval(x))

        for y in self.df.strategies:
            _fields = []
            for x in y:
                name = x.get("name", "")
                params = x.get("params", {})
                for k, v in params.items():
                    field = {"_".join([name, k]) : v}
                    _fields.append(field)
            fields.append(_fields)
        print("end")

df = pd.read_csv("df_spaces_new.csv")

df = Spaces(df)._get_spaces_strategies()
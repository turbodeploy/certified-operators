from datetime import timedelta


class Adjustment:
    """Class that represents an adjustment to be applied to a copied column value."""

    def __init__(self, col_name, delta):
        """Create a new adjustment.

        The adjustment is not applied for the first replica, used as-is for the second,
        doubled for the third, etc.

        :param col_name: name of column to be adjusted
        :param delta: amount of adjustment
        """
        self.col_name = col_name
        self.delta = delta

    def adjust(self, col_list, multiplier):
        """Apply this adjustment to a list of column names, yielding the same list but with
        this adjustment's column name replaced with an SQL expression that will produce the
        adjusted value.

        :param col_list: list of column names (some may already be adjusted)
        :param multiplier: multiplier for adjustment value
        :return: the updated column list
        """
        return [self.__adjust(col, multiplier) for col in col_list]

    def __adjust(self, col, multiplier):
        if col == self.col_name:
            return f"{col} + {self.__adjustment(multiplier * self.delta)}"
        else:
            return col

    @staticmethod
    def __adjustment(a):
        if isinstance(a, timedelta):
            return f"INTERVAL '{a}'"
        else:
            return str(a)

# -* - coding: UTF-8 -* -
from decimal import ROUND_HALF_UP, Decimal


def new_round(num, prec=2):
    return Decimal(str(num)).quantize(Decimal("0." + "0" * prec), rounding=ROUND_HALF_UP)


if __name__ == "__main__":
    print(new_round(0.645))

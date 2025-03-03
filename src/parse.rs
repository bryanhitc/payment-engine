use std::fmt::Display;
use std::ops::{Add, AddAssign, Sub, SubAssign};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

// **Motivation**: it's important that our transaction arithmetic is correct.
// Since floating points can't properly represent all possible numbers (IEEE754),
// and since our input precision is limited to <= 4 digits after the decimal,
// it's better to parse the amounts and convert them to u64s while processing
// the transactions. In a real system, this `Amount` would be parsed immediately
// up front and/or handled by clients, and then the rest of the system would use
// this value interally.

// INVARIANT 1: Amount * MAX_AMOUNT_DECIMAL_SHIFT <= i64::MAX.
// INVARIANT 2: Amount has <= 4 digits after the decimal.
//
// These invariants are enforced via Amount::new. In a real system, these invariants
// should probably only be checked at the creation of this data (e.g., if it's user input).
// Internal services can uphold this invariant, which should allow us to eliminate
// the compute overhead entirely.

#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq)]
pub enum AmountParseError {
    Overflow(f64),
    TooPrecise(f64),
}

impl AmountParseError {
    pub fn to_deserializer_error<E>(&self) -> E
    where
        E: serde::de::Error,
    {
        let (&amount, msg) = match self {
            AmountParseError::Overflow(amount) => {
                (amount, "amount that will not overflow u64 after shift")
            }
            // TODO (CORRECTNESS + MAINTANABILITY): Use Amount::MAX_DIGITS_AFTER_DECIMAL and construct &str at compile time
            AmountParseError::TooPrecise(amount) => (amount, "only 4 digits after decimal"),
        };
        serde::de::Error::invalid_value(serde::de::Unexpected::Float(amount), &msg)
    }
}

impl Display for AmountParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AmountParseError::Overflow(amount) => f.write_fmt(format_args!("Overflow({amount})")),
            AmountParseError::TooPrecise(amount) => {
                f.write_fmt(format_args!("TooPrecise({amount})"))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd)]
pub struct Amount(i64);

impl Amount {
    pub const MAX: Self = Amount(i64::MAX);
    pub const MAX_DIGITS_AFTER_DECIMAL: u32 = 4;

    const MAX_AMOUNT_DECIMAL_SHIFT: f64 = 10u32.pow(Self::MAX_DIGITS_AFTER_DECIMAL) as f64;
    const MAX_F64: f64 = (u64::MAX as f64) / Self::MAX_AMOUNT_DECIMAL_SHIFT;

    pub fn new(amount: f64) -> Result<Amount, AmountParseError> {
        if amount > Self::MAX_F64 {
            return Err(AmountParseError::Overflow(amount));
        }

        let amount_shifted = amount * Self::MAX_AMOUNT_DECIMAL_SHIFT;
        let amount_rounded = amount_shifted.round();
        if (amount_rounded - amount_shifted).abs() > 0.0001 {
            println!("{} = {}", amount_shifted, amount_rounded);
            return Err(AmountParseError::TooPrecise(amount));
        }

        Ok(Amount(amount_rounded as i64))
    }
}

impl From<i64> for Amount {
    fn from(amount: i64) -> Self {
        Amount(amount)
    }
}

impl Add for Amount {
    type Output = Amount;

    fn add(self, rhs: Self) -> Self::Output {
        // ignore overflow
        Amount(self.0 + rhs.0)
    }
}

impl AddAssign for Amount {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0 + rhs.0;
    }
}

impl Sub for Amount {
    type Output = Amount;

    fn sub(self, rhs: Self) -> Self::Output {
        Amount(self.0 - rhs.0)
    }
}

impl SubAssign for Amount {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0;
    }
}

impl Serialize for Amount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Example: 123456 => (123456.0000 / 10000.0) => 12.3456
        let csv_float = (self.0 as f64).round();
        let csv_float_shifted = csv_float / Self::MAX_AMOUNT_DECIMAL_SHIFT;
        serializer.serialize_f64(csv_float_shifted)
    }
}

impl<'de> Deserialize<'de> for Amount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Example: (12.3456 * 10000.0).round() => 123456.0000 => 123456
        let csv_float = f64::deserialize(deserializer)?;
        Amount::new(csv_float).map_err(|err| err.to_deserializer_error::<D::Error>())
    }
}

#[cfg(test)]
mod amount_tests {
    use googletest::prelude::*;

    use crate::parse::{Amount, AmountParseError};

    #[gtest]
    pub fn new_amount_rejects_overflow_before_shift() {
        let overflow_before_shift = u64::MAX as f64;
        expect_that!(
            Amount::new(overflow_before_shift),
            err(eq(AmountParseError::Overflow(overflow_before_shift)))
        );
    }

    #[gtest]
    pub fn new_amount_rejects_overflow_after_shift() {
        let overflow_after_shift = (u64::MAX as f64 / Amount::MAX_AMOUNT_DECIMAL_SHIFT) + 1.0;
        expect_that!(
            Amount::new(overflow_after_shift),
            err(eq(AmountParseError::Overflow(overflow_after_shift)))
        );
    }

    #[gtest]
    pub fn new_amount_rejects_if_still_float_after_shift() {
        expect_that!(
            Amount::new(123.45678),
            err(eq(AmountParseError::TooPrecise(123.45678)))
        );
    }

    #[gtest]
    pub fn new_amount_accepts_zero() {
        expect_that!(Amount::new(0.0), ok(eq(0.into())));
    }

    #[gtest]
    pub fn new_amount_accepts_if_u64_max_after_shift() {
        expect_that!(
            Amount::new(u64::MAX as f64 / Amount::MAX_AMOUNT_DECIMAL_SHIFT),
            ok(eq(i64::MAX.into()))
        )
    }

    #[gtest]
    pub fn new_amount_applies_decimal_shift() {
        expect_that!(Amount::new(123.4567), ok(eq(1234567.into())));
        expect_that!(Amount::new(562.844), ok(eq(5628440.into())));
    }
}

#[cfg(test)]
mod serde_tests {
    use anyhow::Result;
    use serde_test::{Token, assert_de_tokens_error, assert_tokens};

    use crate::parse::*;
    use crate::*;

    #[test]
    pub fn serialize_and_deserialize_amount_transactions() -> Result<()> {
        assert_tokens(
            &Transaction::new(
                1,
                2,
                TransactionType::Withdrawal,
                Some(Amount::new(123.4567)?),
            ),
            &[
                Token::Struct {
                    name: "Transaction",
                    len: 4,
                },
                Token::Str("tx"),
                Token::U32(1),
                Token::Str("client"),
                Token::U16(2),
                Token::Str("type"),
                Token::UnitVariant {
                    name: "TransactionType",
                    variant: "withdrawal",
                },
                Token::Str("amount"),
                Token::Some,
                Token::F64(123.4567),
                Token::StructEnd,
            ],
        );
        Ok(())
    }

    #[test]
    pub fn serialize_and_deserialize_non_amount_transactions() {
        assert_tokens(
            &Transaction::new(1, 2, TransactionType::Resolve, None),
            &[
                Token::Struct {
                    name: "Transaction",
                    len: 4,
                },
                Token::Str("tx"),
                Token::U32(1),
                Token::Str("client"),
                Token::U16(2),
                Token::Str("type"),
                Token::UnitVariant {
                    name: "TransactionType",
                    variant: "resolve",
                },
                Token::Str("amount"),
                Token::None,
                Token::StructEnd,
            ],
        );
    }

    #[test]
    pub fn can_not_serialize_invalid_amount() {
        assert_de_tokens_error::<Transaction>(
            &[
                Token::Struct {
                    name: "Transaction",
                    len: 4,
                },
                Token::Str("tx"),
                Token::U32(1),
                Token::Str("client"),
                Token::U16(2),
                Token::Str("type"),
                Token::UnitVariant {
                    name: "TransactionType",
                    variant: "withdrawal",
                },
                Token::Str("amount"),
                Token::Some,
                Token::F64(123.45678),
                Token::StructEnd,
            ],
            &format!(
                "invalid value: floating point `123.45678`, expected only {} digits after decimal",
                Amount::MAX_DIGITS_AFTER_DECIMAL
            ),
        );
    }
}

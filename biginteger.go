
package cloudwave

import (
	"errors"
	"math/big"
)

func bytes2bigInt(b []byte) (big.Int, error) {
	var bi *big.Int
	length := len(b)
	if length <= 0 {
		bi = new(big.Int).SetInt64(0)
	} else {
		if (b[0] & 0x80) == 0 {
			bi = new(big.Int).SetBytes(b)
		} else { //bi.Neg().neg = false
			minus := true
			for i := length - 1; i >= 0; i-- {
				if minus {
					b[i] = -b[i]
					if b[i] != 0 {
						minus = false
					}
				} else {
					b[i] = ^b[i]
				}
			}
			bb := new(big.Int).SetBytes(b)
			bi = bb.Neg(bb)
		}
	}
	return *bi, nil
}

func bigInt2bytes(bi big.Int) ([]byte, error) {
	b := bi.Bytes()
	length := len(b)
	biSign := bi.Sign()
	if biSign >= 0 {
		if biSign > 0 {
			if (b[0] & 0x80) != 0 {
				buf := make([]byte, length+1)
				buf[0] = 0x00
				copy(buf[1:], b)
				return buf, nil
			}
		}
	} else {
		minus := true
		for i := length - 1; i >= 0; i-- {
			if minus {
				b[i] = -b[i]
				if b[i] != 0 {
					minus = false
				}
			} else {
				b[i] = ^b[i]
			}
		}
		if (b[0] & 0x80) == 0 {
			buf := make([]byte, length+1)
			buf[0] = 0xff
			copy(buf[1:], b)
			return buf, nil
		}
	}
	return b, nil
}

func string2bigInt(s string, scale int) (big.Int, error) {
	var bi *big.Int
	var err error
	err = nil
	var ss string
	slen := len(s)
	if slen <= 0 {
		bi = new(big.Int).SetInt64(0)
	} else {
		p := -1
		for i := 0; i < slen; i++ {
			if s[i] == '.' {
				if p >= 0 {
					err = errors.New("format error")
				}
				p = i
			}
		}
		if err != nil {
			bi = new(big.Int).SetInt64(0)
		} else {
			if p < 0 {
				ss = s
			} else {
				ss = s[0:p]
				if (p + 1) < slen {
					ss += s[p+1 : slen]
				}
				if scale >= (slen - p - 1) {
					for i := 0; i < (scale - slen + p + 1); i++ {
						ss += "0"
					}
				} else {
					ss = ss[0 : p + scale]
				}
			}
			var succeed bool
			bi, succeed = new(big.Int).SetString(ss, 10)
			if !succeed {
				err = errors.New("format error")
			}
		}
	}
	return *bi, err
}

func bigInt2string(bi big.Int, scale int) (string, error) {
	var ss string
	s := bi.String()
	end := len(s)
	start := 0
	if s[0] == '-' || s[0] == '+' {
		start = 1
	}
	if start >= end {
		return "", nil
	}
	if scale <= 0 {
		ss = s
	} else {
		if (end - start) > scale {
			ss = s[0:end-scale] + "." + s[end-scale:end]
		} else {
			ss = s[0:start] + "0."
			for i := 0; i < scale-end+start; i++ {
				ss += "0"
			}
			ss += s[start:end]
		}
		end = len(ss)
		for i := end-1; i >= 0; i-- {
			if ss[i] != '0' {
				end = i + 1
				break;
			}
		}
		if ss[end - 1] == '.' {
			end--
		}
		ss = ss[0 : end]
	}
	return ss, nil
}

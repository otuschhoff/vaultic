package repository

const maxPackSize64 = uint64(MaxPackSize)

type packSizer struct {
	defaultSize uint
	growFactor  uint
	sizeLimit   uint
	currentSize uint64
}

func newPackSizer(defaultSize, growFactor, sizeLimit uint, currentSize uint64) packSizer {
	if sizeLimit == 0 || uint64(sizeLimit) > maxPackSize64 {
		sizeLimit = MaxPackSize
	}
	return packSizer{
		defaultSize: defaultSize,
		growFactor:  growFactor,
		sizeLimit:   sizeLimit,
		currentSize: currentSize,
	}
}

func (s packSizer) target() uint {
	target := uint64(s.defaultSize)
	if s.growFactor != 0 {
		growth := integerSqrt(s.currentSize) * uint64(s.growFactor)
		if ^uint64(0)-target < growth {
			target = ^uint64(0)
		} else {
			target += growth
		}
	}
	if target > uint64(s.sizeLimit) {
		target = uint64(s.sizeLimit)
	}
	if target > maxPackSize64 {
		target = maxPackSize64
	}
	return uint(target)
}

func integerSqrt(value uint64) uint64 {
	if value == 0 {
		return 0
	}
	root := uint64(1) << ((bitLength64(value) + 1) / 2)
	for next := (root + value/root) / 2; next < root; next = (root + value/root) / 2 {
		root = next
	}
	return root
}

func bitLength64(value uint64) uint64 {
	var length uint64
	for value != 0 {
		length++
		value >>= 1
	}
	return length
}

package main

func max(x, y int) int {
	if x >= y {
		return x
	}
	return y
}

func min(x, y int) int {
	if x <= y {
		return x
	}
	return y
}

func jaro(s, a []byte) float64 {
	m := 0
	t := 0
	sl := len(s)
	al := len(a)
	sflags := make([]int, sl, sl)
	aflags := make([]int, al, al)
	_range := int(max(0, max(sl, al)/2-1))

	/* calculate matching characters */
	for i := 0; i < int(al); i++ {
		l := min(i+_range+1, sl)
		for j := max(i-_range, 0); j < l; j++ {
			if a[i] == s[j] && sflags[j] == 0 {
				sflags[j] = 1
				aflags[i] = 1
				m++
				break
			}

		}
	}
	if m == 0 {
		return 0.0
	}

	/* calculate character transpositions */
	l := 0
	j := l
	for i := 0; i < al; i++ {
		if aflags[i] == 1 {
			for j = l; j < sl; j++ {
				if sflags[j] == 1 {
					l = j + 1
					break
				}
			}
			if a[i] != s[j] {
				t++
			}
		}
	}
	t = t / 2

	/* jaro distance */
	fm := float64(m)
	dw := ((fm / float64(sl)) + (fm / float64(al)) + ((fm - float64(t)) / fm)) / 3.0

	return dw

}

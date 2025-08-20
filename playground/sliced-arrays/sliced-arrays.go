package main

import "fmt"

//import "unsafe"

func main() {

	startingArray := [3]int{1, 2, 3}

	fmt.Printf("Created an array with three elements: %v\n\n", startingArray)

	fmt.Printf("Let's do some slicing!\n\n")

	headSlice := startingArray[0:2]
	fmt.Printf("Created Head Slice from the first two array elements : %v\n", headSlice)

	// The first element of both the Array and the Head Slice should be co-located
	fmt.Printf(comparePointers("Array", "Head Slice", &startingArray[0], &headSlice[0]))
	fmt.Println()

	tailSlice := startingArray[1:3]
	fmt.Printf("Created Tail Slice from the last two array elements: %v\n", tailSlice)

	// The last element of both the Array and the Tail Slice should be co-located
	fmt.Printf(comparePointers("Array", "Tail Slice", &startingArray[len(startingArray)-1], &tailSlice[len(tailSlice)-1]))
	fmt.Println()

	headSlice = append(headSlice, 4)
	fmt.Printf("Appended the values 4 to Head Slice : %v\n", headSlice)
	fmt.Printf("Look what happened to Tail Slice! : %v\n", tailSlice)

	// The Head Slice has three elements ...
	// But it also overwrote the second element of the Tail Slice
	fmt.Printf(comparePointers("Head Slice", "Tail Slice", &headSlice[2], &tailSlice[1]))
	fmt.Println()

	tailSlice = append([]int{0}, tailSlice...)
	fmt.Printf("Prepended 0 to Tail Slice : %v\n", tailSlice)
	fmt.Printf("Array is not affected: %v\n", startingArray)
	fmt.Printf("Head Slice is not affected: %v\n", headSlice)

	// If the Array and the Tail Slice were still tied, the last element of both the Array and the Tail Slice would be co-located
	fmt.Printf(comparePointers("Array", "Tail Slice", &startingArray[len(startingArray)-1], &tailSlice[len(tailSlice)-1]))

	// But the Array and the Head Slice are still co-located
	fmt.Printf(comparePointers("Array", "Head Slice", &startingArray[0], &headSlice[0]))
	fmt.Println()

	headSlice = append(headSlice, 5)
	fmt.Printf("Appended 5 to the Head Slice: %v\n", headSlice)

	// Now the Head Slice has grown too long and needs its own allocation
	fmt.Printf(comparePointers("Array", "Head Slice", &startingArray[0], &headSlice[0]))

}

func comparePointers(textA string, textB string, a *int, b *int) string {
	if a == b {
		return fmt.Sprintf("%s and %s share the same memory location: %p\n", textA, textB, a)
	}
	return fmt.Sprintf("%s and %s DO NOT share the same memory location: %p vs %p\n", textA, textB, a, b)

}

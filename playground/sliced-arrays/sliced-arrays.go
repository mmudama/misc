// Here we explore how manipulating slices and arrays can lead to unintuitive results
// Slices are windows into arrays, until they're not
// Author: Monique Mudama ; (c) 2025

package main

import "fmt"

func main() {

	startingArray := [3]int{1, 2, 3}

	fmt.Printf("Created an array with three elements: %v\n", startingArray)

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
	fmt.Printf("Capacities: Array - %d; Head Slice - %d; Tail Slice - %d\n", cap(startingArray), cap(headSlice), cap(tailSlice))
	fmt.Println()

	headSlice = append(headSlice, 4)
	fmt.Printf("Appended the values 4 to Head Slice : %v\n", headSlice)
	fmt.Printf("Look what happened to Tail Slice! : %v\n", tailSlice)
	fmt.Printf("The array changed, too! : %v\n", startingArray)

	// The Head Slice now has three elements ...
	// But it also overwrote the second element of the Tail Slice!
	fmt.Printf(comparePointers("Head Slice", "Tail Slice", &headSlice[2], &tailSlice[1]))
	fmt.Printf(comparePointers("Array", "Head Slice", &startingArray[0], &headSlice[0]))
	fmt.Println()

	tailSlice = append([]int{0}, tailSlice...)
	fmt.Printf("Prepended 0 to Tail Slice : %v\n", tailSlice)
	fmt.Printf("Array is not affected: %v\n", startingArray)
	fmt.Printf("Head Slice is not affected: %v\n", headSlice)

	// The Tail Slice is no longer a window into the Array.
	// This is because append() tries to use the memory allocated to the first argument, and that's not the Tail Slice
	// But even if we were truly appending the new value, the Tail Slice doesn't have enough capacity for another element;
	// it would need to be copied and doubled
	// You can tell that it did NOT copy and double because the capacity is only 3 (same as length), rather than doubling to 4
	// If the Array and the Tail Slice were still tied, the last element of both the Array and the Tail Slice would be co-located
	fmt.Printf(comparePointers("Array", "Tail Slice", &startingArray[len(startingArray)-1], &tailSlice[len(tailSlice)-1]))

	// But the Array and the Head Slice are still co-located
	fmt.Printf(comparePointers("Array", "Head Slice", &startingArray[0], &headSlice[0]))
	fmt.Printf("Capacities: Array - %d; Head Slice - %d; Tail Slice - %d\n", cap(startingArray), cap(headSlice), cap(tailSlice))
	fmt.Println()

	// spoiler: it's true
	if cap(headSlice) == len(headSlice) {
		fmt.Printf("Head Slice will create a new backing array on append; append WILL NOT affect the array\n")
	} else {
		fmt.Printf("Head Slice can still grow; append WILL affect the array\n")
	}

	headSlice = append(headSlice, 5)
	fmt.Printf("Appended 5 to the Head Slice: %v\n", headSlice)

	// The Head Slice's original memory allocation was too small to append any more.
	// Golang doubles its allocation and copies its data to a new location. It's no longer just a window into the Array.
	fmt.Printf(comparePointers("Array", "Head Slice", &startingArray[0], &headSlice[0]))
	fmt.Printf("Capacities: Array - %d; Head Slice - %d; Tail Slice - %d\n", cap(startingArray), cap(headSlice), cap(tailSlice))
	fmt.Printf("Array is not affected: %v\n", startingArray)
	fmt.Println()

	startingArray[2] = 999
	fmt.Printf("Changed the final element of the array: %v\n", startingArray)
	fmt.Printf("Head Slice is not affected: %v\n", headSlice)
}

func comparePointers(textA string, textB string, a *int, b *int) string {
	if a == b {
		//	return fmt.Sprintf("%s and %s share the same memory location: %p\n", textA, textB, a)
		return fmt.Sprintf("%s and %s DO share the same memory location\n", textA, textB)
	}
	//return fmt.Sprintf("%s and %s DO NOT share the same memory location: %p vs %p\n", textA, textB, a, b)
	return fmt.Sprintf("%s and %s DO NOT share the same memory location\n", textA, textB)

}

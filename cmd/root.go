/*
Copyright © 2025 Colin Jacobs <colin@coljac.space>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
package cmd

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

// Global variables for flags
var (
	quietMode bool
	archive   string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "colfs",
	Short: "A file concatenation and management tool",
	Long: `colfs is a CLI tool for combining multiple files into a single blob with an index,
and for managing and restoring files from these blobs.

Examples:
  colfs create --archive myfiles.colfs file1.txt file2.txt dir/file3.txt
  colfs dir --archive myfiles.colfs
  colfs restore --archive myfiles.colfs "*.txt"
  colfs dehydrate --archive myfiles.colfs`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// Check for environment variable if archive not specified
		if archive == "" {
			archive = os.Getenv("COLFS_ARCHIVE")
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Add global flags
	rootCmd.PersistentFlags().BoolVarP(&quietMode, "quiet", "q", false, "Suppress output messages")
	rootCmd.PersistentFlags().StringVarP(&archive, "archive", "a", "", "Archive file (can also be set via COLFS_ARCHIVE environment variable)")

	// Create command
	var createCmd = &cobra.Command{
		Use:   "create [flags] [files...]",
		Short: "Concatenate files into a blob with an index",
		Long: `Concatenate multiple files into a single blob file with an index.
This allows for storing multiple files in one container while preserving their paths.`,
		Run: func(cmd *cobra.Command, args []string) {
			output, _ := cmd.Flags().GetString("output")
			if output != "" {
				archive = output
			}

			deleteAfter, _ := cmd.Flags().GetBool("delete")

			if len(args) == 0 {
				fmt.Println("Error: at least one file must be specified")
				os.Exit(1)
			}

			if !quietMode {
				fmt.Printf("Creating blob at %s with %d files\n", archive, len(args))
				if deleteAfter {
					fmt.Println("Files will be deleted after successful creation")
				}
			}

			// Implementation of create command
			err := createBlob(args, archive, deleteAfter)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				os.Exit(1)
			}
		},
	}

	// Keep 'output' for backward compatibility
	createCmd.Flags().StringP("output", "o", "", "Output file (deprecated, use --archive instead)")
	createCmd.Flags().BoolP("delete", "d", false, "Delete original files after successful creation")

	// Dir command
	var dirCmd = &cobra.Command{
		Use:   "dir",
		Short: "List contents of a blob",
		Long:  `List all files contained within the specified blob.`,
		Run: func(cmd *cobra.Command, args []string) {
			if archive == "" {
				fmt.Println("Error: archive file must be specified with --archive flag or COLFS_ARCHIVE environment variable")
				os.Exit(1)
			}

			if !quietMode {
				fmt.Printf("Listing contents of %s\n", archive)
			}

			err := listBlobContents(archive)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				os.Exit(1)
			}
		},
	}

	// Restore command
	var restoreCmd = &cobra.Command{
		Use:   "restore [pattern]",
		Short: "Restore files matching the pattern",
		Long: `Restore files from a blob that match the specified pattern.
Patterns can be file extensions (*.txt), paths (/root/), or specific filenames.
Use --all to restore all files.`,
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if archive == "" {
				fmt.Println("Error: archive file must be specified with --archive flag or COLFS_ARCHIVE environment variable")
				os.Exit(1)
			}

			allFiles, _ := cmd.Flags().GetBool("all")

			// Check if either a pattern or --all flag is provided
			if len(args) == 0 && !allFiles {
				fmt.Println("Error: either a pattern must be specified or --all flag must be used")
				os.Exit(1)
			}

			pattern := ""
			if len(args) > 0 {
				pattern = args[0]
			}

			if !quietMode {
				if allFiles {
					fmt.Printf("Restoring all files from %s\n", archive)
				} else {
					fmt.Printf("Restoring files from %s matching pattern: %s\n", archive, pattern)
				}
			}

			err := restoreFiles(archive, pattern, allFiles)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				os.Exit(1)
			}
		},
	}

	// Add the all flag to restore command
	restoreCmd.Flags().BoolP("all", "A", false, "Restore all files from the archive")

	// Dehydrate command
	var dehydrateCmd = &cobra.Command{
		Use:   "dehydrate",
		Short: "Delete local files that exist in the blob",
		Long: `For each file in the current working directory, delete it if
the corresponding file exists in the specified blob.`,
		Run: func(cmd *cobra.Command, args []string) {
			if archive == "" {
				fmt.Println("Error: archive file must be specified with --archive flag or COLFS_ARCHIVE environment variable")
				os.Exit(1)
			}

			if !quietMode {
				fmt.Printf("Dehydrating using %s\n", archive)
			}

			recursive, _ := cmd.Flags().GetBool("recursive")
			err := dehydrateFiles(archive, recursive)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				os.Exit(1)
			}
		},
	}

	// Add a recursive flag to the dehydrate command
	dehydrateCmd.Flags().BoolP("recursive", "r", false, "Also remove empty directories after deleting files")

	// Update command
	var updateCmd = &cobra.Command{
		Use:   "update [files...]",
		Short: "Add or update files in an existing archive",
		Long: `Add new files to an existing archive or update files that already exist.
This allows incrementally adding content to archives without recreating them.`,
		Run: func(cmd *cobra.Command, args []string) {
			if archive == "" {
				fmt.Println("Error: archive file must be specified with --archive flag or COLFS_ARCHIVE environment variable")
				os.Exit(1)
			}

			skipExisting, _ := cmd.Flags().GetBool("skip-existing")
			deleteAfter, _ := cmd.Flags().GetBool("delete")

			if len(args) == 0 {
				fmt.Println("Error: at least one file must be specified to update")
				os.Exit(1)
			}

			if !quietMode {
				fmt.Printf("Updating archive %s with %d files\n", archive, len(args))
			}

			err := updateArchive(archive, args, skipExisting, deleteAfter)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				os.Exit(1)
			}
		},
	}

	// Add flags to update command
	updateCmd.Flags().BoolP("skip-existing", "s", false, "Skip files that already exist in the archive")
	updateCmd.Flags().BoolP("delete", "d", false, "Delete original files after successful update")

	// Add commands to root
	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(dirCmd)
	rootCmd.AddCommand(restoreCmd)
	rootCmd.AddCommand(dehydrateCmd)
	rootCmd.AddCommand(updateCmd)
}

// createBlob concatenates files into a single blob with an index
func createBlob(filePaths []string, outputPath string, deleteOriginals bool) error {
	// Open output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// First, reserve space for the index by writing a placeholder
	// We'll come back and fill this in after we know all file sizes
	fmt.Fprintln(outFile, "INDEX_START")

	// We'll build the real index as we go
	var indexContent strings.Builder
	indexContent.WriteString("INDEX_START\n")

	// Current position in the file, starting after we'll write the index
	position := int64(0)

	// Track successfully processed files for potential deletion
	var processedFiles []string

	// Process all files, including those in directories
	allFiles, err := expandFilePaths(filePaths)
	if err != nil {
		return err
	}

	// Write files and build index
	for _, fileEntry := range allFiles {
		// Add entry to index
		indexContent.WriteString(fmt.Sprintf("%s|%d|%d\n", fileEntry.path, fileEntry.size, position))

		// Read and write file content
		content, err := os.ReadFile(fileEntry.path)
		if err != nil {
			return fmt.Errorf("could not read file %s: %w", fileEntry.path, err)
		}

		// Write content to output file
		_, err = outFile.Write(content)
		if err != nil {
			return fmt.Errorf("could not write file %s content: %w", fileEntry.path, err)
		}

		// Update position for next file
		position += fileEntry.size

		// Track this file for potential deletion
		processedFiles = append(processedFiles, fileEntry.path)

		if !quietMode {
			fmt.Printf("Added file: %s (%d bytes)\n", fileEntry.path, fileEntry.size)
		}
	}

	// Finish the index
	indexContent.WriteString("INDEX_END\n")

	// Get current position (end of all content)
	currentPos, err := outFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get current file position: %w", err)
	}

	// Go back to beginning of file to write the real index
	_, err = outFile.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to beginning of file: %w", err)
	}

	// Write the actual index
	_, err = outFile.WriteString(indexContent.String())
	if err != nil {
		return fmt.Errorf("failed to write index: %w", err)
	}

	// Return to the end of the file (important to ensure we don't truncate)
	_, err = outFile.Seek(currentPos, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to return to end of file: %w", err)
	}

	// Delete original files if requested
	if deleteOriginals {
		for _, path := range processedFiles {
			err := os.Remove(path)
			if err != nil && !quietMode {
				fmt.Printf("Warning: could not delete original file %s: %v\n", path, err)
			} else if !quietMode {
				fmt.Printf("Deleted original file: %s\n", path)
			}
		}
	}

	if !quietMode {
		fmt.Printf("Successfully created blob at %s with %d files\n", outputPath, len(processedFiles))
	}
	return nil
}

// fileEntry represents a file to be added to the blob
type fileEntry struct {
	path string
	size int64
}

// expandFilePaths processes the input paths and returns a list of all files,
// recursively traversing directories
func expandFilePaths(paths []string) ([]fileEntry, error) {
	var result []fileEntry

	for _, path := range paths {
		fileInfo, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("could not stat path %s: %w", path, err)
		}

		if !fileInfo.IsDir() {
			// It's a regular file, add it directly
			result = append(result, fileEntry{
				path: path,
				size: fileInfo.Size(),
			})
			continue
		}

		// It's a directory, walk it recursively
		fmt.Printf("Processing directory: %s\n", path)
		err = filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Skip directories themselves - we only want the files inside
			if info.IsDir() {
				return nil
			}

			result = append(result, fileEntry{
				path: filePath,
				size: info.Size(),
			})
			return nil
		})

		if err != nil {
			return nil, fmt.Errorf("error walking directory %s: %w", path, err)
		}
	}

	return result, nil
}

// blobIndex represents an entry in the blob index
type blobIndex struct {
	filePath string
	size     int64
	offset   int64
}

// parseIndex reads and parses the index section of a blob file
func parseIndex(blobPath string) ([]blobIndex, error) {
	file, err := os.Open(blobPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open blob file: %w", err)
	}
	defer file.Close()

	var entries []blobIndex
	scanner := bufio.NewScanner(file)

	// Find the index start
	foundStart := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "INDEX_START" {
			foundStart = true
			break
		}
	}

	if !foundStart {
		return nil, fmt.Errorf("invalid blob format: INDEX_START not found")
	}

	// Parse index entries
	for scanner.Scan() {
		line := scanner.Text()
		if line == "INDEX_END" {
			break
		}

		parts := strings.Split(line, "|")
		if len(parts) != 3 {
			continue // Skip invalid lines
		}

		filePath := parts[0]
		size, err := parseSizeString(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid size in index: %w", err)
		}

		offset, err := parseSizeString(parts[2])
		if err != nil {
			return nil, fmt.Errorf("invalid offset in index: %w", err)
		}

		entries = append(entries, blobIndex{
			filePath: filePath,
			size:     size,
			offset:   offset,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading blob index: %w", err)
	}

	return entries, nil
}

// parseSizeString converts a string to int64
func parseSizeString(s string) (int64, error) {
	var size int64
	_, err := fmt.Sscanf(s, "%d", &size)
	return size, err
}

// listBlobContents displays the contents of a blob file
func listBlobContents(blobPath string) error {
	entries, err := parseIndex(blobPath)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		if !quietMode {
			fmt.Println("Blob file contains no entries.")
		}
		return nil
	}

	var totalSize int64

	if !quietMode {
		fmt.Println("Files in blob:")
		fmt.Println("---------------------------------------")
		fmt.Printf("%-40s %15s\n", "FILE PATH", "SIZE")
		fmt.Println("---------------------------------------")
	}

	for _, entry := range entries {
		if !quietMode {
			fmt.Printf("%-40s %15d bytes\n", entry.filePath, entry.size)
		} else {
			// Even in quiet mode, we print just the filenames one per line
			fmt.Println(entry.filePath)
		}
		totalSize += entry.size
	}

	if !quietMode {
		fmt.Println("---------------------------------------")
		fmt.Printf("Total: %d files, %d bytes\n", len(entries), totalSize)
	}

	return nil
}

// restoreFiles extracts files from a blob that match the given pattern
func restoreFiles(blobPath string, pattern string, allFiles bool) error {
	entries, err := parseIndex(blobPath)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return fmt.Errorf("blob file contains no entries")
	}

	// Open the blob file for reading
	blobFile, err := os.Open(blobPath)
	if err != nil {
		return fmt.Errorf("failed to open blob file: %w", err)
	}
	defer blobFile.Close()

	var restoredCount int

	// Process each entry
	for _, entry := range entries {
		// Check if the file matches the pattern or if --all is specified
		if !allFiles {
			matched, err := matchPattern(entry.filePath, pattern)
			if err != nil {
				return fmt.Errorf("pattern matching error: %w", err)
			}

			if !matched {
				continue
			}
		}

		// Create parent directories if they don't exist
		dir := filepath.Dir(entry.filePath)
		if dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", dir, err)
			}
		}

		// Create the output file
		outFile, err := os.Create(entry.filePath)
		if err != nil {
			return fmt.Errorf("failed to create output file %s: %w", entry.filePath, err)
		}

		// Seek to the correct position in the blob
		// First, calculate the position after the index
		indexEndPos, err := findIndexEndPosition(blobFile)
		if err != nil {
			outFile.Close()
			return err
		}

		// Then add the offset of this specific file
		_, err = blobFile.Seek(indexEndPos+entry.offset, io.SeekStart)
		if err != nil {
			outFile.Close()
			return fmt.Errorf("failed to seek in blob file: %w", err)
		}

		// Read and write the file data
		buffer := make([]byte, 4096) // 4KB buffer
		var bytesWritten int64

		for bytesWritten < entry.size {
			bytesToRead := min(int64(len(buffer)), entry.size-bytesWritten)
			n, err := blobFile.Read(buffer[:bytesToRead])
			if err != nil && err != io.EOF {
				outFile.Close()
				return fmt.Errorf("error reading from blob: %w", err)
			}

			if n == 0 {
				break // EOF
			}

			_, err = outFile.Write(buffer[:n])
			if err != nil {
				outFile.Close()
				return fmt.Errorf("error writing to output file: %w", err)
			}

			bytesWritten += int64(n)
		}

		outFile.Close()
		restoredCount++
		if !quietMode {
			fmt.Printf("Restored: %s (%d bytes)\n", entry.filePath, entry.size)
		}
	}

	if !quietMode {
		fmt.Printf("Restore complete: %d files restored\n", restoredCount)
	}
	return nil
}

// findIndexEndPosition locates the end of the index section in the blob
func findIndexEndPosition(file *os.File) (int64, error) {
	// Reset to beginning of file
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to beginning of file: %w", err)
	}

	scanner := bufio.NewScanner(file)
	var pos int64

	// Skip to the end of the index
	for scanner.Scan() {
		line := scanner.Text()
		pos += int64(len(line)) + 1 // +1 for newline

		if line == "INDEX_END" {
			return pos, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error scanning blob file: %w", err)
	}

	return 0, fmt.Errorf("INDEX_END not found in blob file")
}

// matchPattern checks if a file path matches the given pattern
func matchPattern(filePath, pattern string) (bool, error) {
	// Check for direct filename match
	if pattern == path.Base(filePath) {
		return true, nil
	}

	// Check for extension match (*.ext)
	if strings.HasPrefix(pattern, "*.") {
		ext := strings.TrimPrefix(pattern, "*")
		return strings.HasSuffix(filePath, ext), nil
	}

	// Check for directory/path prefix match
	if strings.HasSuffix(pattern, "/") || strings.HasSuffix(pattern, "\\") {
		return strings.HasPrefix(filePath, pattern), nil
	}

	// Use filepath.Match for glob patterns
	return filepath.Match(pattern, filePath)
}

// min returns the minimum of two int64 values
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// dehydrateFiles deletes local files that exist in the archive with the same size
func dehydrateFiles(blobPath string, recursive bool) error {
	// Get the list of files in the archive
	entries, err := parseIndex(blobPath)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return fmt.Errorf("blob file contains no entries")
	}

	// Get current working directory for relative path calculations
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current working directory: %w", err)
	}

	var deletedCount int
	var skippedCount int

	// Check each file in the archive
	for _, entry := range entries {
		// Create the local path for this file (relative to current directory)
		// Fix: use clean paths to ensure consistent path separators
		localPath := filepath.Join(cwd, filepath.Clean(entry.filePath))

		// Get file info if it exists
		fileInfo, err := os.Stat(localPath)
		if err != nil {
			if os.IsNotExist(err) {
				// File doesn't exist locally, skip
				if !quietMode {
					fmt.Printf("Skipped: %s (file not found)\n", entry.filePath)
				}
				continue
			}
			// Other error
			if !quietMode {
				fmt.Printf("Warning: could not access %s: %v\n", entry.filePath, err)
			}
			skippedCount++
			continue
		}

		// Check if the file size matches
		if fileInfo.Size() == entry.size {
			// Delete the file
			err = os.Remove(localPath)
			if err != nil {
				if !quietMode {
					fmt.Printf("Warning: could not delete %s: %v\n", entry.filePath, err)
				}
				skippedCount++
			} else {
				if !quietMode {
					fmt.Printf("Deleted: %s (%d bytes)\n", entry.filePath, entry.size)
				}
				deletedCount++
			}
		} else {
			if !quietMode {
				fmt.Printf("Skipped: %s (size mismatch: %d vs %d bytes)\n",
					entry.filePath, fileInfo.Size(), entry.size)
			}
			skippedCount++
		}
	}

	if !quietMode {
		fmt.Printf("Dehydrate complete: %d files deleted, %d files skipped\n",
			deletedCount, skippedCount)
	}

	// After deleting all matching files, clean up empty directories if requested
	if recursive && deletedCount > 0 {
		cleanupEmptyDirs(cwd)
	}

	return nil
}

// cleanupEmptyDirs removes empty directories, starting from the deepest ones
func cleanupEmptyDirs(rootDir string) {
	var totalRemoved int
	
	// Make multiple passes until no more directories can be removed
	for {
		removedInPass := 0
		
		// Build a list of all directories
		var dirs []string
		filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil // Skip errors
			}
			if info.IsDir() && path != rootDir {
				dirs = append(dirs, path)
			}
			return nil
		})
		
		// No directories left to process
		if len(dirs) == 0 {
			break
		}
		
		// Sort directories by depth (deepest first)
		sort.Slice(dirs, func(i, j int) bool {
			// More path separators means deeper directory
			return strings.Count(dirs[i], string(os.PathSeparator)) > 
				   strings.Count(dirs[j], string(os.PathSeparator))
		})
		
		// Try to remove each directory
		for _, dir := range dirs {
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) > 0 {
				continue // Skip non-empty or inaccessible directories
			}
			
			if err := os.Remove(dir); err == nil {
				removedInPass++
				totalRemoved++
				if !quietMode {
					fmt.Printf("Removed empty directory: %s\n", dir)
				}
			}
		}
		
		// If we didn't remove any directories in this pass, we're done
		if removedInPass == 0 {
			break
		}
	}
	
	if !quietMode && totalRemoved > 0 {
		fmt.Printf("Directory cleanup complete: %d empty directories removed\n", totalRemoved)
	}
}

// updateArchive adds or updates files in an existing archive
func updateArchive(archivePath string, filePaths []string, skipExisting bool, deleteOriginals bool) error {
	// Check if the archive exists
	_, err := os.Stat(archivePath)
	if err != nil {
		if os.IsNotExist(err) {
			if !quietMode {
				fmt.Printf("Archive %s does not exist. Creating new archive instead.\n", archivePath)
			}
			return createBlob(filePaths, archivePath, deleteOriginals)
		}
		return fmt.Errorf("failed to access archive: %w", err)
	}

	// Parse the existing index
	entries, err := parseIndex(archivePath)
	if err != nil {
		return fmt.Errorf("failed to parse archive index: %w", err)
	}

	// Create a map of existing files for faster lookup
	existingFiles := make(map[string]blobIndex)
	for _, entry := range entries {
		existingFiles[entry.filePath] = entry
	}

	// Process all files to be added/updated
	allFiles, err := expandFilePaths(filePaths)
	if err != nil {
		return err
	}

	// Create a temp file to hold the updated archive
	tempFile, err := os.CreateTemp("", "colfs-update-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tempPath := tempFile.Name()
	defer func() {
		tempFile.Close()
		// In case of errors, try to clean up the temp file
		os.Remove(tempPath)
	}()

	// Set up the new index
	var indexContent strings.Builder
	indexContent.WriteString("INDEX_START\n")

	// First, open the existing archive for reading
	srcArchive, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}
	defer srcArchive.Close()

	// Find the index end position to know where the file contents start
	indexEndPos, err := findIndexEndPosition(srcArchive)
	if err != nil {
		return fmt.Errorf("failed to find end of index: %w", err)
	}

	// Start building the new archive
	fmt.Fprintln(tempFile, "INDEX_START")

	// Track files we've processed for potential deletion
	var processedFiles []string

	// Track statistics
	var addedCount, updatedCount, skippedCount int

	// Map to track files we'll be including (to avoid duplicates)
	includedFiles := make(map[string]bool)

	// First, copy all existing files we're keeping to the new archive
	currentPos := int64(0)
	for _, entry := range entries {
		// Check if this file is being updated
		needsUpdate := false
		for _, newFile := range allFiles {
			if newFile.path == entry.filePath {
				needsUpdate = true
				break
			}
		}

		if needsUpdate && !skipExisting {
			// We'll update this file later, don't include the original
			if !quietMode {
				fmt.Printf("Will update: %s\n", entry.filePath)
			}
			continue
		}

		// This file stays the same - add it to the new index
		indexContent.WriteString(fmt.Sprintf("%s|%d|%d\n", entry.filePath, entry.size, currentPos))
		includedFiles[entry.filePath] = true

		// Copy the file data from the original archive
		_, err = srcArchive.Seek(indexEndPos+entry.offset, io.SeekStart)
		if err != nil {
			return fmt.Errorf("failed to seek in source archive: %w", err)
		}

		// Copy the file content
		_, err = io.CopyN(tempFile, srcArchive, entry.size)
		if err != nil {
			return fmt.Errorf("failed to copy file data: %w", err)
		}

		// Update position for the next file
		currentPos += entry.size
	}

	// Now add all the new/updated files
	for _, fileEntry := range allFiles {
		// Skip if we already included this file (can happen with skipExisting)
		if includedFiles[fileEntry.path] {
			skippedCount++
			if !quietMode {
				fmt.Printf("Skipped: %s (already in archive)\n", fileEntry.path)
			}
			continue
		}

		// Check if this is an update or a new file
		_, isUpdate := existingFiles[fileEntry.path]

		// Add entry to the new index
		indexContent.WriteString(fmt.Sprintf("%s|%d|%d\n", fileEntry.path, fileEntry.size, currentPos))
		includedFiles[fileEntry.path] = true

		// Read and write file content
		content, err := os.ReadFile(fileEntry.path)
		if err != nil {
			return fmt.Errorf("could not read file %s: %w", fileEntry.path, err)
		}

		// Write content to output file
		_, err = tempFile.Write(content)
		if err != nil {
			return fmt.Errorf("could not write file %s content: %w", fileEntry.path, err)
		}

		// Update position for next file
		currentPos += fileEntry.size

		// Track this file for potential deletion
		processedFiles = append(processedFiles, fileEntry.path)

		if isUpdate {
			updatedCount++
			if !quietMode {
				fmt.Printf("Updated: %s (%d bytes)\n", fileEntry.path, fileEntry.size)
			}
		} else {
			addedCount++
			if !quietMode {
				fmt.Printf("Added: %s (%d bytes)\n", fileEntry.path, fileEntry.size)
			}
		}
	}

	// Finish the index
	indexContent.WriteString("INDEX_END\n")

	// Get current position (end of all content)
	currentPos, err = tempFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get current file position: %w", err)
	}

	// Go back to beginning of file to write the real index
	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to beginning of file: %w", err)
	}

	// Write the actual index
	_, err = tempFile.WriteString(indexContent.String())
	if err != nil {
		return fmt.Errorf("failed to write index: %w", err)
	}

	// Close both files
	tempFile.Close()
	srcArchive.Close()

	// Replace the original with the updated version
	err = os.Rename(tempPath, archivePath)
	if err != nil {
		return fmt.Errorf("failed to replace archive with updated version: %w", err)
	}

	// Delete original files if requested
	if deleteOriginals {
		for _, path := range processedFiles {
			err := os.Remove(path)
			if err != nil && !quietMode {
				fmt.Printf("Warning: could not delete original file %s: %v\n", path, err)
			} else if !quietMode {
				fmt.Printf("Deleted original file: %s\n", path)
			}
		}
	}

	if !quietMode {
		fmt.Printf("Update complete: %d added, %d updated, %d skipped\n",
			addedCount, updatedCount, skippedCount)
	}

	return nil
}

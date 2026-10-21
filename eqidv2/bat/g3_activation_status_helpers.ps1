function Read-StrictLiteralKeyValueFile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [string[]]$RequiredKeys
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Key/value evidence is missing: $Path"
    }

    $raw = Get-Content -LiteralPath $Path -Raw -Encoding UTF8 -ErrorAction Stop
    $lines = @([regex]::Split($raw, "`r`n|`n|`r"))
    $values = @{}

    for ($index = 0; $index -lt $lines.Count; $index++) {
        $line = [string]$lines[$index]
        $lineNumber = $index + 1

        # A normal text file ends with one newline, which produces one final
        # empty split element.  Empty lines anywhere else are malformed.
        if ($line.Length -eq 0 -and $index -eq ($lines.Count - 1)) {
            continue
        }
        if ([string]::IsNullOrWhiteSpace($line)) {
            throw "Malformed key/value evidence at line ${lineNumber}: blank lines are not allowed."
        }

        $separator = $line.IndexOf('=')
        if ($separator -le 0) {
            throw "Malformed key/value evidence at line ${lineNumber}: expected key=value."
        }

        $key = $line.Substring(0, $separator)
        $value = $line.Substring($separator + 1)
        if ($key -notmatch '^[A-Za-z][A-Za-z0-9_]*$') {
            throw "Malformed key/value evidence at line ${lineNumber}: invalid key."
        }
        if ($values.ContainsKey($key)) {
            throw "Duplicate key in key/value evidence at line ${lineNumber}: $key"
        }

        # Deliberately do not unescape or normalize the value.  In particular,
        # Windows paths such as C:\Users\... must remain byte-for-character
        # literal instead of being interpreted as PowerShell escape sequences.
        $values.Add($key, $value)
    }

    foreach ($requiredKey in $RequiredKeys) {
        if ($requiredKey -notmatch '^[A-Za-z][A-Za-z0-9_]*$') {
            throw "Invalid required key name: $requiredKey"
        }
        if (-not $values.ContainsKey($requiredKey) -or
            [string]::IsNullOrWhiteSpace([string]$values[$requiredKey])) {
            throw "Required key is missing or blank in key/value evidence: $requiredKey"
        }
    }

    return $values
}

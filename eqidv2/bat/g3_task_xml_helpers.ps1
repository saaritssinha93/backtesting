# Pure XML conversion. Loading this helper never accesses Task Scheduler.
function ConvertTo-G3ScheduledTaskXml {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)][string]$SourceXml,
        [Parameter(Mandatory=$true)][string]$NewTaskName,
        [Parameter(Mandatory=$true)][string]$Launcher
    )
    [xml]$xml = $SourceXml
    $taskNamespace = 'http://schemas.microsoft.com/windows/2004/02/mit/task'
    if ($xml.DocumentElement.LocalName -ne 'Task' -or $xml.DocumentElement.NamespaceURI -ne $taskNamespace) {
        throw 'Unsupported Task Scheduler XML root or namespace.'
    }
    $ns = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
    $ns.AddNamespace('t', $taskNamespace)
    $actions = $xml.SelectSingleNode('/t:Task/t:Actions', $ns)
    $exec = $xml.SelectSingleNode('/t:Task/t:Actions/t:Exec', $ns)
    if ($null -eq $exec -or $xml.SelectNodes('/t:Task/t:Actions/*', $ns).Count -ne 1) {
        throw 'Only one standard Exec action is supported.'
    }
    $command = $exec.SelectSingleNode('t:Command', $ns)
    if ($null -eq $command) { throw 'Required task Exec/Command is missing.' }
    $command.InnerText = "$env:SystemRoot\System32\cmd.exe"
    $arg = $xml.CreateElement('Arguments', $taskNamespace)
    $arg.InnerText = '/d /c ""' + $Launcher + '""'
    $prior = $exec.SelectSingleNode('t:Arguments', $ns)
    if ($null -ne $prior) {
        $exec.ReplaceChild($arg, $prior) | Out-Null
    } else {
        $working = $exec.SelectSingleNode('t:WorkingDirectory', $ns)
        if ($null -ne $working) { $exec.InsertBefore($arg, $working) | Out-Null }
        else { $exec.AppendChild($arg) | Out-Null }
    }
    # Settings and Enabled may be omitted when the source uses schema defaults.
    $settings = $xml.SelectSingleNode('/t:Task/t:Settings', $ns)
    if ($null -eq $settings) {
        $settings = $xml.CreateElement('Settings', $taskNamespace)
        $next = $xml.SelectSingleNode('/t:Task/t:Data', $ns)
        if ($null -eq $next) { $next = $actions }
        $xml.DocumentElement.InsertBefore($settings, $next) | Out-Null
    }
    $enabled = $settings.SelectSingleNode('t:Enabled', $ns)
    if ($null -eq $enabled) {
        $enabled = $xml.CreateElement('Enabled', $taskNamespace)
        $settings.AppendChild($enabled) | Out-Null
    }
    $enabled.InnerText = 'false'
    # URI metadata is optional; add it without depending on XML property casts.
    $registration = $xml.SelectSingleNode('/t:Task/t:RegistrationInfo', $ns)
    if ($null -eq $registration) {
        $registration = $xml.CreateElement('RegistrationInfo', $taskNamespace)
        $xml.DocumentElement.PrependChild($registration) | Out-Null
    }
    $uri = $registration.SelectSingleNode('t:URI', $ns)
    if ($null -eq $uri) {
        $uri = $xml.CreateElement('URI', $taskNamespace)
        $registration.AppendChild($uri) | Out-Null
    }
    $uri.InnerText = '\' + $NewTaskName
    return $xml.OuterXml
}

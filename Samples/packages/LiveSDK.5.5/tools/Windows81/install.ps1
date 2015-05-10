param($installPath, $toolsPath, $package, $project)

switch ($project.Kind.ToUpperInvariant()) {
    "{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}" {
        # C#
        $dllPath = join-path $installPath "WindowsXAML\Microsoft.Live.dll"
        $project.Object.References.Add($dllPath)
    }
    "{262852C6-CD72-467D-83FE-5EEB1973A190}" { 
        # JavaScript    
        $jsFolderProjectItem = $project.ProjectItems.Item("js")
        if ($jsFolderProjectItem -eq $null) {
            $kind = $null
            $project.ProjectItems.AddFolder("js", $kind)
            $jsFolderProjectItem = $project.ProjectItems.Item("js")
        }
        
        $jsFilePath = join-path $installPath "WindowsHTML\wl.js"
        try {
            $jsFolderProjectItem.ProjectItems.AddFromFileCopy($jsFilePath)
        }
        catch {
            write-host "Could not add $jsFilePath to project folder $($jsFolderProjectItem.Name)"
        }
    }
}

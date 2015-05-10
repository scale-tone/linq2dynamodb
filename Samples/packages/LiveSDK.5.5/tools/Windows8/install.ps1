param($installPath, $toolsPath, $package, $project)

switch ($project.Type) {
    "C#" {
        $dllPath = join-path $installPath "WindowsXAML\Microsoft.Live.dll"
        $project.Object.References.Add($dllPath)
    }
    "JavaScript" {
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

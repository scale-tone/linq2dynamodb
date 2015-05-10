param($installPath, $toolsPath, $package, $project)

switch ($project.Kind.ToUpperInvariant()) {
    "{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}" {
        # C#
        $project.Object.References | Where-Object { $_.Name -eq 'Microsoft.Live' } | ForEach-Object { $_.Remove() }
    }
    "{262852C6-CD72-467D-83FE-5EEB1973A190}" { 
        # JavaScript
        try {
            $jsFolderProjectItem = $project.ProjectItems.Item("js")
        } catch {
            write-host "Unable to find js directory."
            return
        }

        try {
            $jsFileProjectItem = $jsFolderProjectItem.ProjectItems.Item("wl.js")
        } catch {
            write-host "Unable to find wl.js."
            return
        }

        $jsFileProjectItem.Delete()
    }
}

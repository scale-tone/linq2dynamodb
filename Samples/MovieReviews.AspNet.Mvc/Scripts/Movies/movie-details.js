function MovieDetailsViewModel() {
    //data
    var self = this;
    self.content = ko.observable();
    self.showAction = ko.observable(true);

    self.loadReviews = function (movieId) {
        $.ajax({
            url: "/Movies/Details/" + movieId + "/Reviews",
            method: "GET",
            success: function(data) {
                self.content(data);
                self.showAction(false);
            }
        });
    };
}

$(function() {
    var vm = new MovieDetailsViewModel();
    ko.applyBindings(vm);
});
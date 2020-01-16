-- invalid means that the reservation's template was deleted.
ALTER TABLE `reservation` MODIFY `status` ENUM('reserved','future','invalid') NOT NULL;